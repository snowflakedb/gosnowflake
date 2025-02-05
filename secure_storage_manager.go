// Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/99designs/keyring"
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
	credCacheFilePath string
	localCredCache    map[string]string
	credCacheLock     sync.RWMutex
}

func newFileBasedSecureStorageManager() (*fileBasedSecureStorageManager, error) {
	ssm := &fileBasedSecureStorageManager{
		localCredCache: map[string]string{},
		credCacheLock:  sync.RWMutex{},
	}
	credCacheDir := ssm.buildCredCacheDirPath()
	if err := ssm.createCacheDir(credCacheDir); err != nil {
		return nil, err
	}
	credCacheFilePath := filepath.Join(credCacheDir, credCacheFileName)
	logger.Infof("Credentials cache path: %v", credCacheFilePath)
	ssm.credCacheFilePath = credCacheFilePath
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

func (ssm *fileBasedSecureStorageManager) buildCredCacheDirPath() string {
	credCacheDir := os.Getenv(credCacheDirEnv)
	if credCacheDir != "" {
		return credCacheDir
	}
	home := os.Getenv("HOME")
	if home == "" {
		logger.Info("HOME is blank")
		return ""
	}
	credCacheDir = filepath.Join(home, ".cache", "snowflake")
	return credCacheDir
}

func (ssm *fileBasedSecureStorageManager) setCredential(tokenSpec *secureTokenSpec, value string) {
	if value == "" {
		logger.Debug("no token provided")
	} else {
		credentialsKey := tokenSpec.buildKey()
		ssm.credCacheLock.Lock()
		defer ssm.credCacheLock.Unlock()
		ssm.localCredCache[credentialsKey] = value

		j, err := json.Marshal(ssm.localCredCache)
		if err != nil {
			logger.Warnf("failed to convert credential to JSON.")
			return
		}

		logger.Debugf("writing credential cache file. %v\n", ssm.credCacheFilePath)
		credCacheLockFileName := ssm.credCacheFilePath + ".lck"
		logger.Debugf("Creating lock file. %v", credCacheLockFileName)
		err = os.Mkdir(credCacheLockFileName, 0600)

		switch {
		case os.IsExist(err):
			statinfo, err := os.Stat(credCacheLockFileName)
			if err != nil {
				logger.Debugf("failed to write credential cache file. file: %v, err: %v. ignored.\n", ssm.credCacheFilePath, err)
				return
			}
			if time.Since(statinfo.ModTime()) < 15*time.Minute {
				logger.Debugf("other process locks the cache file. %v. ignored.\n", ssm.credCacheFilePath)
				return
			}
			if err = os.Remove(credCacheLockFileName); err != nil {
				logger.Debugf("failed to delete lock file. file: %v, err: %v. ignored.\n", credCacheLockFileName, err)
				return
			}
			if err = os.Mkdir(credCacheLockFileName, 0600); err != nil {
				logger.Debugf("failed to delete lock file. file: %v, err: %v. ignored.\n", credCacheLockFileName, err)
				return
			}
		}
		defer os.RemoveAll(credCacheLockFileName)

		if err = os.WriteFile(ssm.credCacheFilePath, j, 0644); err != nil {
			logger.Debugf("Failed to write the cache file. File: %v err: %v.", ssm.credCacheFilePath, err)
		}
	}
}

func (ssm *fileBasedSecureStorageManager) getCredential(tokenSpec *secureTokenSpec) string {
	credentialsKey := tokenSpec.buildKey()
	ssm.credCacheLock.Lock()
	defer ssm.credCacheLock.Unlock()
	localCredCache := ssm.readTemporaryCacheFile()
	cred := localCredCache[credentialsKey]
	if cred != "" {
		logger.Debug("Successfully read token. Returning as string")
	} else {
		logger.Debug("Returned credential is empty")
	}
	return cred
}

func (ssm *fileBasedSecureStorageManager) readTemporaryCacheFile() map[string]string {
	jsonData, err := os.ReadFile(ssm.credCacheFilePath)
	if err != nil {
		logger.Debugf("Failed to read credential file: %v", err)
		return nil
	}
	err = json.Unmarshal([]byte(jsonData), &ssm.localCredCache)
	if err != nil {
		logger.Debugf("failed to read JSON. Err: %v", err)
		return nil
	}

	return ssm.localCredCache
}

func (ssm *fileBasedSecureStorageManager) deleteCredential(tokenSpec *secureTokenSpec) {
	ssm.credCacheLock.Lock()
	defer ssm.credCacheLock.Unlock()
	credentialsKey := tokenSpec.buildKey()
	delete(ssm.localCredCache, credentialsKey)
	j, err := json.Marshal(ssm.localCredCache)
	if err != nil {
		logger.Warnf("failed to convert credential to JSON.")
		return
	}
	ssm.writeTemporaryCacheFile(j)
}

func (ssm *fileBasedSecureStorageManager) writeTemporaryCacheFile(input []byte) {
	logger.Debugf("writing credential cache file. %v\n", ssm.credCacheFilePath)
	credCacheLockFileName := ssm.credCacheFilePath + ".lck"
	err := os.Mkdir(credCacheLockFileName, 0600)
	logger.Debugf("Creating lock file. %v", credCacheLockFileName)

	switch {
	case os.IsExist(err):
		statinfo, err := os.Stat(credCacheLockFileName)
		if err != nil {
			logger.Debugf("failed to write credential cache file. file: %v, err: %v. ignored.\n", ssm.credCacheFilePath, err)
			return
		}
		if time.Since(statinfo.ModTime()) < 15*time.Minute {
			logger.Debugf("other process locks the cache file. %v. ignored.\n", ssm.credCacheFilePath)
			return
		}
		if err = os.Remove(credCacheLockFileName); err != nil {
			logger.Debugf("failed to delete lock file. file: %v, err: %v. ignored.\n", credCacheLockFileName, err)
			return
		}
		if err = os.Mkdir(credCacheLockFileName, 0600); err != nil {
			logger.Debugf("failed to delete lock file. file: %v, err: %v. ignored.\n", credCacheLockFileName, err)
			return
		}
	}
	defer os.RemoveAll(credCacheLockFileName)

	if err = os.WriteFile(ssm.credCacheFilePath, input, 0644); err != nil {
		logger.Debugf("Failed to write the cache file. File: %v err: %v.", ssm.credCacheFilePath, err)
	}
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
	return host + ":" + user + ":" + driverName + ":" + credTypeStr
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

func (ssm *noopSecureStorageManager) deleteCredential(_ *secureTokenSpec) { //TODO implement me
}
