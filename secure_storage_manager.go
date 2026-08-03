package gosnowflake

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	sfconfig "github.com/snowflakedb/gosnowflake/v2/internal/config"
)

type tokenType string

const (
	idToken           tokenType = "ID_TOKEN"
	mfaToken          tokenType = "MFA_TOKEN"
	oauthAccessToken  tokenType = "OAUTH_ACCESS_TOKEN"
	oauthRefreshToken tokenType = "OAUTH_REFRESH_TOKEN"
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

type secureTokenSpec interface {
	buildKey() (string, error)
}

// Fields must remain in lexicographic order to match the cross-driver hash contract.
type mfaIDKeyData struct {
	Snowflake string `json:"snowflake"`
	Username  string `json:"username"`
}

// Fields must remain in lexicographic order to match the cross-driver hash contract.
type oauthKeyData struct {
	Idp       string `json:"idp"`
	Role      string `json:"role"`
	Snowflake string `json:"snowflake"`
	Username  string `json:"username"`
}

type hostUserTokenSpec struct {
	tokenType tokenType
	snowflake string
	username  string
}

func (s *hostUserTokenSpec) buildKey() (string, error) {
	if s.snowflake == "" {
		return "", errors.New("snowflake URL is required for token cache key")
	}
	if s.username == "" {
		return "", errors.New("username is required for token cache key")
	}
	return marshalAndFinalize(s.tokenType, mfaIDKeyData{
		Snowflake: normalizeURL(s.snowflake),
		Username:  normalizeIdentifier(s.username),
	})
}

func (s *hostUserTokenSpec) lockID() string {
	return s.snowflake + "|" + s.username + "|" + string(s.tokenType)
}

type oauthTokenSpec struct {
	tokenType tokenType
	idp       string
	snowflake string
	username  string
	role      string
}

func (s *oauthTokenSpec) buildKey() (string, error) {
	if s.snowflake == "" {
		return "", errors.New("snowflake URL is required for token cache key")
	}
	if s.username == "" {
		return "", errors.New("username is required for token cache key")
	}
	if s.idp == "" {
		return "", errors.New("idp URL is required for OAuth token cache key")
	}
	return marshalAndFinalize(s.tokenType, oauthKeyData{
		Idp:       normalizeURL(s.idp),
		Role:      normalizeIdentifier(s.role),
		Snowflake: normalizeURL(s.snowflake),
		Username:  normalizeIdentifier(s.username),
	})
}

func (s *oauthTokenSpec) lockID() string {
	return s.idp + "|" + s.snowflake + "|" + s.username + "|" + s.role + "|" + string(s.tokenType)
}

// finalizeCacheKey produces the versioned, SHA-256-hashed cache key
//
//	SnowflakeTokenCache.v2.<TOKEN_TYPE>.<lowercase_sha256(canonical_json(keyData))>
//
// The token type lives in the readable key prefix and is never part of keyData.
// The canonical JSON (compact, keys sorted lexicographically) is identical across
// all Snowflake drivers, enabling cross-driver token reuse.
func finalizeCacheKey(tt tokenType, jsonBytes []byte) string {
	sum := sha256.Sum256(jsonBytes)
	hexHash := hex.EncodeToString(sum[:])
	return "SnowflakeTokenCache.v2." + string(tt) + "." + hexHash
}

func marshalAndFinalize(tt tokenType, keyData any) (string, error) {
	jsonBytes, err := json.Marshal(keyData)
	if err != nil {
		return "", fmt.Errorf("failed to serialize cache key: %w", err)
	}
	return finalizeCacheKey(tt, jsonBytes), nil
}

func newMfaTokenSpec(cfg *Config) *hostUserTokenSpec {
	return &hostUserTokenSpec{
		tokenType: mfaToken,
		snowflake: cfg.Host,
		username:  cfg.User,
	}
}

func newIDTokenSpec(cfg *Config) *hostUserTokenSpec {
	return &hostUserTokenSpec{
		tokenType: idToken,
		snowflake: cfg.Host,
		username:  cfg.User,
	}
}

func newOAuthAccessTokenSpec(cfg *Config) *oauthTokenSpec {
	return &oauthTokenSpec{
		tokenType: oauthAccessToken,
		idp:       oauthTokenRequestURL(cfg),
		snowflake: cfg.Host,
		username:  cfg.User,
		role:      cfg.Role,
	}
}

func newOAuthRefreshTokenSpec(cfg *Config) *oauthTokenSpec {
	return &oauthTokenSpec{
		tokenType: oauthRefreshToken,
		idp:       oauthTokenRequestURL(cfg),
		snowflake: cfg.Host,
		username:  cfg.User,
		role:      cfg.Role,
	}
}

type secureStorageManager interface {
	setCredential(tokenSpec secureTokenSpec, value string)
	getCredential(tokenSpec secureTokenSpec) string
	deleteCredential(tokenSpec secureTokenSpec)
}

var credentialsStorage = newSecureStorageManager()

func newSecureStorageManager() secureStorageManager {
	return defaultOsSpecificSecureStorageManager()
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
	parentOfCacheDir := cacheDir[:strings.LastIndex(cacheDir, "/")]

	if err = os.MkdirAll(parentOfCacheDir, os.FileMode(0755)); err != nil {
		return "", err
	}

	// We don't check if permissions are incorrect here if a directory exists, because we check it later.
	if err = os.Mkdir(cacheDir, os.FileMode(0700)); err != nil && !errors.Is(err, os.ErrExist) {
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

func (ssm *fileBasedSecureStorageManager) getTokens(data map[string]any) map[string]any {
	val, ok := data["tokens"]
	if !ok {
		return map[string]any{}
	}

	tokens, ok := val.(map[string]any)
	if !ok {
		return map[string]any{}
	}

	return tokens
}

func (ssm *fileBasedSecureStorageManager) withLock(action func(cacheFile *os.File)) {
	err := ssm.lockFile()
	if err != nil {
		logger.Warnf("Unable to lock cache. %v", err)
		return
	}
	defer ssm.unlockFile()

	ssm.withCacheFile(action)
}

func (ssm *fileBasedSecureStorageManager) withCacheFile(action func(*os.File)) {
	cacheFile, err := os.OpenFile(ssm.credFilePath(), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		logger.Warnf("cannot access %v. %v", ssm.credFilePath(), err)
		return
	}
	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			logger.Warnf("cannot release file descriptor for %v. %v", ssm.credFilePath(), err)
		}
	}(cacheFile)

	cacheDir, err := os.Open(ssm.credDirPath)
	if err != nil {
		logger.Warnf("cannot access %v. %v", ssm.credDirPath, err)
	}
	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			logger.Warnf("cannot release file descriptor for %v. %v", cacheDir, err)
		}
	}(cacheDir)

	if sfconfig.ShouldSkipTokenFilePermissionsVerification() {
		logger.Debugf("Skipping credential cache permission verification because SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION=true")
		action(cacheFile)
		return
	}

	if err := ensureFileOwner(cacheFile); err != nil {
		logger.Warnf("failed to ensure owner for temporary cache file. %v", err)
		return
	}
	if err := ensureFilePermissions(cacheFile, 0600); err != nil {
		logger.Warnf("failed to ensure permission for temporary cache file. %v", err)
		return
	}
	if err := ensureFileOwner(cacheDir); err != nil {
		logger.Warnf("failed to ensure owner for temporary cache dir. %v", err)
		return
	}
	if err := ensureFilePermissions(cacheDir, 0700|os.ModeDir); err != nil {
		logger.Warnf("failed to ensure permission for temporary cache dir. %v", err)
		return
	}

	action(cacheFile)
}

func (ssm *fileBasedSecureStorageManager) setCredential(tokenSpec secureTokenSpec, value string) {
	if value == "" {
		logger.Debug("no token provided")
		return
	}
	credentialsKey, err := tokenSpec.buildKey()
	if err != nil {
		logger.Warnf("cannot build token spec: %v", err)
		return
	}

	ssm.withLock(func(cacheFile *os.File) {
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
		} else {
			logger.Debugf("Set credential succeeded. Key: %v, file location: %v", credentialsKey, ssm.credFilePath())
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

	lockFile, err := os.Open(lockPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to open %v. err: %v", lockPath, err)
	}
	defer func() {
		if lockFile != nil {
			err = lockFile.Close()
			if err != nil {
				logger.Debugf("error while closing lock file. %v", err)
			}
		}
	}()

	if err == nil { // file exists
		fileInfo, err := lockFile.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat %v and determine if lock is stale. err: %v", lockPath, err)
		}

		if !sfconfig.ShouldSkipTokenFilePermissionsVerification() {
			ownerUID, err := provideFileOwner(lockFile)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			currentUser, err := user.Current()
			if err != nil {
				return err
			}
			if strconv.Itoa(int(ownerUID)) != currentUser.Uid {
				return errors.New("incorrect owner of " + lockFile.Name())
			}
		}

		// removing stale lock
		now := time.Now()
		if fileInfo.ModTime().Add(time.Second).UnixNano() < now.UnixNano() {
			logger.Debugf("removing credentials cache lock file, stale for %vms", (now.UnixNano()-fileInfo.ModTime().UnixNano())/1000/1000)
			err = os.Remove(lockPath)
			if err != nil {
				return fmt.Errorf("failed to remove %v while trying to remove stale lock. err: %v", lockPath, err)
			}
		}
	}

	locked := false
	for range numRetries {
		err := os.Mkdir(lockPath, 0700)
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

func (ssm *fileBasedSecureStorageManager) getCredential(tokenSpec secureTokenSpec) string {
	credentialsKey, err := tokenSpec.buildKey()
	if err != nil {
		logger.Warnf("cannot build token spec: %v", err)
		return ""
	}

	ret := ""
	ssm.withLock(func(cacheFile *os.File) {
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

func (ssm *fileBasedSecureStorageManager) readTemporaryCacheFile(cacheFile *os.File) (map[string]any, error) {

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
		return map[string]any{}, fmt.Errorf("failed to unmarshal credential cache file. %v", err)
	}

	return credentialsMap, nil
}

func (ssm *fileBasedSecureStorageManager) deleteCredential(tokenSpec secureTokenSpec) {
	credentialsKey, err := tokenSpec.buildKey()
	if err != nil {
		logger.Warnf("cannot build token spec: %v", err)
		return
	}

	ssm.withLock(func(cacheFile *os.File) {
		credCache, err := ssm.readTemporaryCacheFile(cacheFile)
		if err != nil {
			logger.Warnf("Error while reading cache file. %v", err)
			return
		}
		delete(ssm.getTokens(credCache), credentialsKey)

		err = ssm.writeTemporaryCacheFile(credCache, cacheFile)
		if err != nil {
			logger.Warnf("Set credential failed. Unable to write cache. %v", err)
		} else {
			logger.Debugf("Deleted credential succeeded. Key: %v, file location: %v", credentialsKey, ssm.credFilePath())
		}
	})
}

func (ssm *fileBasedSecureStorageManager) writeTemporaryCacheFile(cache map[string]any, cacheFile *os.File) error {
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
	return nil
}

// normalizeURL strips the scheme and userinfo, drops query/fragment,
// trims a root-only trailing slash, and uppercases the remainder.
func normalizeURL(rawURL string) string {
	s := rawURL
	if idx := strings.Index(s, "://"); idx >= 0 {
		s = s[idx+3:]
	}
	if idx := strings.Index(s, "@"); idx >= 0 {
		s = s[idx+1:]
	}
	s = strings.SplitN(s, "?", 2)[0]
	s = strings.SplitN(s, "#", 2)[0]
	s = strings.TrimRight(s, "/")
	return strings.ToUpper(s)
}

// normalizeIdentifier uppercases characters outside double-quoted segments
// and preserves the contents of "..." segments verbatim.
func normalizeIdentifier(id string) string {
	var b strings.Builder
	inQuotes := false
	for _, ch := range id {
		switch {
		case ch == '"':
			inQuotes = !inQuotes
			b.WriteRune(ch)
		case inQuotes:
			b.WriteRune(ch)
		default:
			b.WriteRune(unicode.ToUpper(ch))
		}
	}
	return b.String()
}

type noopSecureStorageManager struct {
}

func newNoopSecureStorageManager() *noopSecureStorageManager {
	return &noopSecureStorageManager{}
}

func (ssm *noopSecureStorageManager) setCredential(_ secureTokenSpec, _ string) {
}

func (ssm *noopSecureStorageManager) getCredential(_ secureTokenSpec) string {
	return ""
}

func (ssm *noopSecureStorageManager) deleteCredential(_ secureTokenSpec) {
}

type threadSafeSecureStorageManager struct {
	mu       *sync.Mutex
	delegate secureStorageManager
}

func (ssm *threadSafeSecureStorageManager) setCredential(tokenSpec secureTokenSpec, value string) {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	ssm.delegate.setCredential(tokenSpec, value)
}

func (ssm *threadSafeSecureStorageManager) getCredential(tokenSpec secureTokenSpec) string {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	return ssm.delegate.getCredential(tokenSpec)
}

func (ssm *threadSafeSecureStorageManager) deleteCredential(tokenSpec secureTokenSpec) {
	ssm.mu.Lock()
	defer ssm.mu.Unlock()
	ssm.delegate.deleteCredential(tokenSpec)
}
