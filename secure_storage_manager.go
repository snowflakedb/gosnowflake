package gosnowflake

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"sync"
)

type tokenType string

const (
	idToken           tokenType = "ID_TOKEN"
	mfaToken          tokenType = "MFA_TOKEN"
	oauthAccessToken  tokenType = "OAUTH_ACCESS_TOKEN"
	oauthRefreshToken tokenType = "OAUTH_REFRESH_TOKEN"
)

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

func newOAuthAccessTokenSpec(host, user string) *secureTokenSpec {
	return &secureTokenSpec{
		host,
		user,
		oauthAccessToken,
	}
}

func newOAuthRefreshTokenSpec(host, user string) *secureTokenSpec {
	return &secureTokenSpec{
		host,
		user,
		oauthRefreshToken,
	}
}

type secureStorageManager interface {
	setCredential(tokenSpec *secureTokenSpec, value string)
	getCredential(tokenSpec *secureTokenSpec) string
	deleteCredential(tokenSpec *secureTokenSpec)
}

var credentialsStorage = newSecureStorageManager()

func newSecureStorageManager() secureStorageManager {
	return osSpecificSecureStorageManager()
}

func ensureFileOwner(f *os.File) error {
	ownerUID, err := provideFileOwner(f)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	currentUser, err := user.Current()
	if err != nil {
		return err
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if strconv.Itoa(int(ownerUID)) != currentUser.Uid {
		return errors.New("incorrect owner of " + f.Name())
	}
	return nil
}

func ensureFilePermissions(f *os.File, expectedMode os.FileMode) error {
	fileInfo, err := f.Stat()
	if err != nil {
		return err
	}
	if fileInfo.Mode().Perm() != expectedMode&os.ModePerm {
		return fmt.Errorf("incorrect permissions(%v, expected %v) for credential file", fileInfo.Mode(), expectedMode)
	}
	return nil
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
