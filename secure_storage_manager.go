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

const (
	driverName        = "SNOWFLAKE-GO-DRIVER"
	credCacheDirEnv   = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR"
	credCacheFileName = "temporary_credential.json"
)

type secureStorageManager interface {
	setCredential(sc *snowflakeConn, credType, token string)
	getCredential(sc *snowflakeConn, credType string)
	deleteCredential(sc *snowflakeConn, credType string)
}

var credentialsStorage = newSecureStorageManager()

func newSecureStorageManager() secureStorageManager {
	switch runtime.GOOS {
	case "linux":
		return newFileBasedSecureStorageManager()
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

func newFileBasedSecureStorageManager() secureStorageManager {
	ssm := &fileBasedSecureStorageManager{
		localCredCache: map[string]string{},
		credCacheLock:  sync.RWMutex{},
	}
	credCacheDir := ssm.buildCredCacheDirPath()
	if err := ssm.createCacheDir(credCacheDir); err != nil {
		logger.Debugf("failed to create credentials cache dir. %v", err)
		return newNoopSecureStorageManager()
	}
	credCacheFilePath := filepath.Join(credCacheDir, credCacheFileName)
	logger.Infof("Credentials cache path: %v", credCacheFilePath)
	ssm.credCacheFilePath = credCacheFilePath
	return ssm
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

func (ssm *fileBasedSecureStorageManager) setCredential(sc *snowflakeConn, credType, token string) {
	if token == "" {
		logger.Debug("no token provided")
	} else {
		target := buildCredentialsKey(sc.cfg.Host, sc.cfg.User, credType)
		ssm.credCacheLock.Lock()
		defer ssm.credCacheLock.Unlock()
		ssm.localCredCache[target] = token

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

func (ssm *fileBasedSecureStorageManager) getCredential(sc *snowflakeConn, credType string) {
	target := buildCredentialsKey(sc.cfg.Host, sc.cfg.User, credType)
	ssm.credCacheLock.Lock()
	defer ssm.credCacheLock.Unlock()
	localCredCache := ssm.readTemporaryCacheFile()
	cred := localCredCache[target]
	if cred != "" {
		logger.Debug("Successfully read token. Returning as string")
	} else {
		logger.Debug("Returned credential is empty")
	}

	if credType == idToken {
		sc.cfg.IDToken = cred
	} else if credType == mfaToken {
		sc.cfg.MfaToken = cred
	} else {
		logger.Debugf("Unrecognized type %v for local cached credential", credType)
	}
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

func (ssm *fileBasedSecureStorageManager) deleteCredential(sc *snowflakeConn, credType string) {
	ssm.credCacheLock.Lock()
	defer ssm.credCacheLock.Unlock()
	target := buildCredentialsKey(sc.cfg.Host, sc.cfg.User, credType)
	delete(ssm.localCredCache, target)
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

func newKeyringBasedSecureStorageManager() secureStorageManager {
	return &keyringSecureStorageManager{}
}

func (ssm *keyringSecureStorageManager) setCredential(sc *snowflakeConn, credType, token string) {
	if token == "" {
		logger.Debug("no token provided")
	} else {
		var target string
		if runtime.GOOS == "windows" {
			target = driverName + ":" + credType
			ring, _ := keyring.Open(keyring.Config{
				WinCredPrefix: strings.ToUpper(sc.cfg.Host),
				ServiceName:   strings.ToUpper(sc.cfg.User),
			})
			item := keyring.Item{
				Key:  target,
				Data: []byte(token),
			}
			if err := ring.Set(item); err != nil {
				logger.Debugf("Failed to write to Windows credential manager. Err: %v", err)
			}
		} else if runtime.GOOS == "darwin" {
			target = buildCredentialsKey(sc.cfg.Host, sc.cfg.User, credType)
			ring, _ := keyring.Open(keyring.Config{
				ServiceName: target,
			})
			account := strings.ToUpper(sc.cfg.User)
			item := keyring.Item{
				Key:  account,
				Data: []byte(token),
			}
			if err := ring.Set(item); err != nil {
				logger.Debugf("Failed to write to keychain. Err: %v", err)
			}
		}
	}
}

func (ssm *keyringSecureStorageManager) getCredential(sc *snowflakeConn, credType string) {
	var target string
	cred := ""
	if runtime.GOOS == "windows" {
		target = driverName + ":" + credType
		ring, _ := keyring.Open(keyring.Config{
			WinCredPrefix: strings.ToUpper(sc.cfg.Host),
			ServiceName:   strings.ToUpper(sc.cfg.User),
		})
		i, err := ring.Get(target)
		if err != nil {
			logger.Debugf("Failed to read target or could not find it in Windows Credential Manager. Error: %v", err)
		}
		cred = string(i.Data)
	} else if runtime.GOOS == "darwin" {
		target = buildCredentialsKey(sc.cfg.Host, sc.cfg.User, credType)
		ring, _ := keyring.Open(keyring.Config{
			ServiceName: target,
		})
		account := strings.ToUpper(sc.cfg.User)
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

	if credType == idToken {
		sc.cfg.IDToken = cred
	} else if credType == mfaToken {
		sc.cfg.MfaToken = cred
	} else {
		logger.Debugf("Unrecognized type %v for local cached credential", credType)
	}
}

func (ssm *keyringSecureStorageManager) deleteCredential(sc *snowflakeConn, credType string) {
	target := driverName + ":" + credType
	if runtime.GOOS == "windows" {
		ring, _ := keyring.Open(keyring.Config{
			WinCredPrefix: strings.ToUpper(sc.cfg.Host),
			ServiceName:   strings.ToUpper(sc.cfg.User),
		})
		err := ring.Remove(target)
		if err != nil {
			logger.Debugf("Failed to delete target in Windows Credential Manager. Error: %v", err)
		}
	} else if runtime.GOOS == "darwin" {
		target = buildCredentialsKey(sc.cfg.Host, sc.cfg.User, credType)
		ring, _ := keyring.Open(keyring.Config{
			ServiceName: target,
		})
		account := strings.ToUpper(sc.cfg.User)
		err := ring.Remove(account)
		if err != nil {
			logger.Debugf("Failed to delete target in keychain. Error: %v", err)
		}
	}
}

func buildCredentialsKey(host, user, credType string) string {
	host = strings.ToUpper(host)
	user = strings.ToUpper(user)
	credType = strings.ToUpper(credType)
	target := host + ":" + user + ":" + driverName + ":" + credType
	return target
}

type noopSecureStorageManager struct {
}

func newNoopSecureStorageManager() secureStorageManager {
	return &noopSecureStorageManager{}
}

func (ssm *noopSecureStorageManager) setCredential(sc *snowflakeConn, credType, token string) {
}

func (ssm *noopSecureStorageManager) getCredential(sc *snowflakeConn, credType string) {
}

func (ssm *noopSecureStorageManager) deleteCredential(sc *snowflakeConn, credType string) { //TODO implement me
}
