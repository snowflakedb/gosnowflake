//go:build !linux

package gosnowflake

import (
	"runtime"
	"sync"

	"github.com/99designs/keyring"
)

func defaultOsSpecificSecureStorageManager() secureStorageManager {
	switch runtime.GOOS {
	case "darwin", "windows":
		// The opt-in is darwin only. Windows is excluded on both counts: its
		// credential manager is not keyed by the calling binary's code
		// signature, so it does not have the problem this solves, and the file
		// based manager's directory lookup is POSIX specific --
		// defaultLinuxCacheDirConf reads HOME rather than USERPROFILE, and
		// lookupCacheDir splits on "/" while filepath.Join emits "\" there.
		if runtime.GOOS == "darwin" && useFileCredentialCache() {
			ssm, err := newFileBasedSecureStorageManager()
			if err != nil {
				// Deliberately not falling back to the keyring. The opt-in is
				// explicit, and its whole purpose is to keep credentials out of
				// the keyring, so quietly selecting it here would reintroduce
				// the prompts the caller set the variable to avoid. Degrade to
				// no caching instead, matching what linux does on the same
				// failure.
				logger.Warnf("%v is enabled but the credentials cache dir could not be created: %v. Not storing credentials locally.", useFileCredCacheEnv, err)
				return newNoopSecureStorageManager()
			}
			logger.Debugf("%v is enabled, using file based secure storage manager.", useFileCredCacheEnv)
			return &threadSafeSecureStorageManager{&sync.Mutex{}, ssm}
		}
		logger.Debugf("OS is %v, using keyring based secure storage manager.", runtime.GOOS)
		return &threadSafeSecureStorageManager{&sync.Mutex{}, newKeyringBasedSecureStorageManager()}
	default:
		logger.Debugf("OS %v does not support credentials cache", runtime.GOOS)
		return newNoopSecureStorageManager()
	}
}

type keyringSecureStorageManager struct {
}

func newKeyringBasedSecureStorageManager() *keyringSecureStorageManager {
	return &keyringSecureStorageManager{}
}

func (ssm *keyringSecureStorageManager) setCredential(tokenSpec secureTokenSpec, value string) {
	if value == "" {
		logger.Debug("no token provided")
	} else {
		credentialsKey, err := tokenSpec.buildKey()
		if err != nil {
			logger.Warnf("cannot build token spec: %v", err)
			return
		}
		switch runtime.GOOS {
		case "windows":
			ring, _ := keyring.Open(keyring.Config{
				WinCredPrefix: credentialsKey,
				ServiceName:   credentialsKey,
			})
			item := keyring.Item{
				Key:  credentialsKey,
				Data: []byte(value),
			}
			if err := ring.Set(item); err != nil {
				logger.Debugf("Failed to write to Windows credential manager. Err: %v", err)
			}
		case "darwin":
			ring, _ := keyring.Open(keyring.Config{
				ServiceName: credentialsKey,
			})
			item := keyring.Item{
				Key:  credentialsKey,
				Data: []byte(value),
			}
			if err := ring.Set(item); err != nil {
				logger.Debugf("Failed to write to keychain. Err: %v", err)
			}
		}
	}
}

func (ssm *keyringSecureStorageManager) getCredential(tokenSpec secureTokenSpec) string {
	cred := ""
	credentialsKey, err := tokenSpec.buildKey()
	if err != nil {
		logger.Warnf("cannot build token spec: %v", err)
		return ""
	}
	switch runtime.GOOS {
	case "windows":
		ring, _ := keyring.Open(keyring.Config{
			WinCredPrefix: credentialsKey,
			ServiceName:   credentialsKey,
		})
		i, err := ring.Get(credentialsKey)
		if err != nil {
			logger.Debugf("Failed to read credentialsKey or could not find it in Windows Credential Manager. Error: %v", err)
		}
		cred = string(i.Data)
	case "darwin":
		ring, _ := keyring.Open(keyring.Config{
			ServiceName: credentialsKey,
		})
		i, err := ring.Get(credentialsKey)
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

func (ssm *keyringSecureStorageManager) deleteCredential(tokenSpec secureTokenSpec) {
	credentialsKey, err := tokenSpec.buildKey()
	if err != nil {
		logger.Warnf("cannot build token spec: %v", err)
		return
	}
	switch runtime.GOOS {
	case "windows":
		ring, _ := keyring.Open(keyring.Config{
			WinCredPrefix: credentialsKey,
			ServiceName:   credentialsKey,
		})
		err := ring.Remove(credentialsKey)
		if err != nil {
			logger.Debugf("Failed to delete credentialsKey in Windows Credential Manager. Error: %v", err)
		}
	case "darwin":
		ring, _ := keyring.Open(keyring.Config{
			ServiceName: credentialsKey,
		})
		err := ring.Remove(credentialsKey)
		if err != nil {
			logger.Debugf("Failed to delete credentialsKey in keychain. Error: %v", err)
		}
	}
}
