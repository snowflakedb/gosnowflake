package gosnowflake

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	sfconfig "github.com/snowflakedb/gosnowflake/v2/internal/config"
)

func TestBuildCredCacheDirPath(t *testing.T) {
	skipOnWindows(t, "permission model is different")
	testRoot1, err := os.MkdirTemp("", "")
	assertNilF(t, err)
	defer os.RemoveAll(testRoot1)
	testRoot2, err := os.MkdirTemp("", "")
	assertNilF(t, err)
	defer os.RemoveAll(testRoot2)

	env1 := overrideEnv("CACHE_DIR_TEST_NOT_EXISTING", "/tmp/not_existing_dir")
	defer env1.rollback()
	env2 := overrideEnv("CACHE_DIR_TEST_1", testRoot1)
	defer env2.rollback()
	env3 := overrideEnv("CACHE_DIR_TEST_2", testRoot2)
	defer env3.rollback()

	t.Run("cannot find any dir", func(t *testing.T) {
		_, err := buildCredCacheDirPath([]cacheDirConf{
			{envVar: "CACHE_DIR_TEST_NOT_EXISTING"},
		})
		assertEqualE(t, err.Error(), "no credentials cache directory found")
		_, err = os.Stat("/tmp/not_existing_dir")
		assertStringContainsE(t, err.Error(), "no such file or directory")
	})

	t.Run("should use first dir that exists", func(t *testing.T) {
		path, err := buildCredCacheDirPath([]cacheDirConf{
			{envVar: "CACHE_DIR_TEST_NOT_EXISTING"},
			{envVar: "CACHE_DIR_TEST_1"},
		})
		assertNilF(t, err)
		assertEqualE(t, path, testRoot1)
		stat, err := os.Stat(testRoot1)
		assertNilF(t, err)
		assertEqualE(t, stat.Mode(), 0700|os.ModeDir)
	})

	t.Run("should use first dir that exists and append segments", func(t *testing.T) {
		path, err := buildCredCacheDirPath([]cacheDirConf{
			{envVar: "CACHE_DIR_TEST_NOT_EXISTING"},
			{envVar: "CACHE_DIR_TEST_2", pathSegments: []string{"sub1", "sub2"}},
		})
		assertNilF(t, err)
		assertEqualE(t, path, filepath.Join(testRoot2, "sub1", "sub2"))
		stat, err := os.Stat(testRoot2)
		assertNilF(t, err)
		assertEqualE(t, stat.Mode(), 0700|os.ModeDir)
	})
}

func TestCredCacheFileName(t *testing.T) {
	t.Run("defaults to credential_cache_v2.json", func(t *testing.T) {
		env := overrideEnv(credCacheFileNameEnv, "")
		defer env.rollback()
		assertEqualE(t, credCacheFileName(), "credential_cache_v2.json")
	})

	t.Run("honors SF_TEMPORARY_CREDENTIAL_CACHE_FILE_NAME", func(t *testing.T) {
		env := overrideEnv(credCacheFileNameEnv, "custom_cache.json")
		defer env.rollback()
		assertEqualE(t, credCacheFileName(), "custom_cache.json")
	})

	t.Run("empty override keeps the default file name", func(t *testing.T) {
		skipOnWindows(t, "permission model is different")
		credCacheDir, err := os.MkdirTemp("", "")
		assertNilF(t, err)
		defer os.RemoveAll(credCacheDir)
		dirEnv := overrideEnv(credCacheDirEnv, credCacheDir)
		defer dirEnv.rollback()
		nameEnv := overrideEnv(credCacheFileNameEnv, "")
		defer nameEnv.rollback()

		ssm, err := newFileBasedSecureStorageManager()
		assertNilF(t, err)
		assertEqualE(t, ssm.credFilePath, filepath.Join(credCacheDir, "credential_cache_v2.json"))
		assertEqualE(t, ssm.lockPath, filepath.Join(credCacheDir, "credential_cache_v2.json.lck"))
	})

	t.Run("file name is fixed when the manager is created", func(t *testing.T) {
		skipOnWindows(t, "permission model is different")
		credCacheDir, err := os.MkdirTemp("", "")
		assertNilF(t, err)
		defer os.RemoveAll(credCacheDir)
		dirEnv := overrideEnv(credCacheDirEnv, credCacheDir)
		defer dirEnv.rollback()
		nameEnv := overrideEnv(credCacheFileNameEnv, "first.json")
		defer nameEnv.rollback()

		ssm, err := newFileBasedSecureStorageManager()
		assertNilF(t, err)
		assertNilF(t, os.Setenv(credCacheFileNameEnv, "second.json"))
		tokenSpec := newMfaTokenSpec(&Config{Host: "host.com", User: "johndoe"})
		ssm.setCredential(tokenSpec, "token")

		assertEqualE(t, ssm.credFilePath, filepath.Join(credCacheDir, "first.json"))
		assertEqualE(t, ssm.lockPath, filepath.Join(credCacheDir, "first.json.lck"))
		assertEqualE(t, ssm.getCredential(tokenSpec), "token")
		_, err = os.Stat(filepath.Join(credCacheDir, "second.json"))
		assertTrueE(t, os.IsNotExist(err))
		_, err = os.Stat(filepath.Join(credCacheDir, "second.json.lck"))
		assertTrueE(t, os.IsNotExist(err))
	})

	t.Run("override changes cache and lock paths", func(t *testing.T) {
		skipOnWindows(t, "permission model is different")
		credCacheDir, err := os.MkdirTemp("", "")
		assertNilF(t, err)
		defer os.RemoveAll(credCacheDir)
		dirEnv := overrideEnv(credCacheDirEnv, credCacheDir)
		defer dirEnv.rollback()
		nameEnv := overrideEnv(credCacheFileNameEnv, "my_cache.json")
		defer nameEnv.rollback()

		ssm, err := newFileBasedSecureStorageManager()
		assertNilF(t, err)
		assertEqualE(t, ssm.credFilePath, filepath.Join(credCacheDir, "my_cache.json"))
		assertEqualE(t, ssm.lockPath, filepath.Join(credCacheDir, "my_cache.json.lck"))
	})
}

func TestBuildCredCacheDirPathFromEnv(t *testing.T) {
	skipOnWindows(t, "permission model is different")

	t.Run("empty SF_TEMPORARY_CREDENTIAL_CACHE_DIR falls through to XDG_CACHE_HOME", func(t *testing.T) {
		xdgRoot, err := os.MkdirTemp("", "")
		assertNilF(t, err)
		defer os.RemoveAll(xdgRoot)
		dirEnv := overrideEnv(credCacheDirEnv, "")
		defer dirEnv.rollback()
		xdgEnv := overrideEnv("XDG_CACHE_HOME", xdgRoot)
		defer xdgEnv.rollback()

		path, err := buildCredCacheDirPath(defaultLinuxCacheDirConf)
		assertNilF(t, err)
		assertEqualE(t, path, filepath.Join(xdgRoot, "snowflake"))
	})

	t.Run("SF_TEMPORARY_CREDENTIAL_CACHE_DIR set to a file is a hard error", func(t *testing.T) {
		tmp, err := os.MkdirTemp("", "")
		assertNilF(t, err)
		defer os.RemoveAll(tmp)
		notADir := filepath.Join(tmp, "notadir")
		assertNilF(t, os.WriteFile(notADir, []byte("x"), 0600))
		xdgRoot := filepath.Join(tmp, "xdg")
		assertNilF(t, os.Mkdir(xdgRoot, 0700))

		dirEnv := overrideEnv(credCacheDirEnv, notADir)
		defer dirEnv.rollback()
		xdgEnv := overrideEnv("XDG_CACHE_HOME", xdgRoot)
		defer xdgEnv.rollback()

		_, err = buildCredCacheDirPath(defaultLinuxCacheDirConf)
		assertNotNilF(t, err)
		assertStringContainsE(t, err.Error(), "is not a directory")
		_, statErr := os.Stat(filepath.Join(xdgRoot, "snowflake"))
		assertTrueE(t, os.IsNotExist(statErr))
	})

	t.Run("ignores XDG_CACHE_DIR and falls back to HOME", func(t *testing.T) {
		tmp, err := os.MkdirTemp("", "")
		assertNilF(t, err)
		defer os.RemoveAll(tmp)
		xdgDir := filepath.Join(tmp, "xdg_dir")
		assertNilF(t, os.Mkdir(xdgDir, 0700))
		homeDir := filepath.Join(tmp, "home")
		assertNilF(t, os.Mkdir(homeDir, 0700))

		dirEnv := overrideEnv(credCacheDirEnv, "")
		defer dirEnv.rollback()
		legacyXdgEnv := overrideEnv("XDG_CACHE_DIR", xdgDir)
		defer legacyXdgEnv.rollback()
		xdgHomeEnv := overrideEnv("XDG_CACHE_HOME", "")
		defer xdgHomeEnv.rollback()
		homeEnv := overrideEnv("HOME", homeDir)
		defer homeEnv.rollback()

		path, err := buildCredCacheDirPath(defaultLinuxCacheDirConf)
		assertNilF(t, err)
		assertEqualE(t, path, filepath.Join(homeDir, ".cache", "snowflake"))
	})
}

func TestSnowflakeFileBasedSecureStorageManager(t *testing.T) {
	skipOnWindows(t, "file system permission is different")
	credCacheDir, err := os.MkdirTemp("", "")
	assertNilF(t, err)
	defer os.RemoveAll(credCacheDir)
	assertNilF(t, os.MkdirAll(credCacheDir, os.ModePerm))
	credCacheDirEnvOverride := overrideEnv(credCacheDirEnv, credCacheDir)
	defer credCacheDirEnvOverride.rollback()
	nameEnv := overrideEnv(credCacheFileNameEnv, "")
	defer nameEnv.rollback()
	ssm, err := newFileBasedSecureStorageManager()
	assertNilF(t, err)

	t.Run("store single token", func(t *testing.T) {
		tokenSpec := newMfaTokenSpec(&Config{Host: "host.com", User: "johndoe"})
		cred := "token123"
		ssm.setCredential(tokenSpec, cred)
		assertEqualE(t, ssm.getCredential(tokenSpec), cred)
		ssm.deleteCredential(tokenSpec)
		assertEqualE(t, ssm.getCredential(tokenSpec), "")
		lockInfo, err := os.Stat(ssm.lockPath)
		assertNilF(t, err)
		assertTrueE(t, lockInfo.Mode().IsRegular())
		assertFalseE(t, lockInfo.IsDir())
	})

	t.Run("store tokens of different types, hosts and users", func(t *testing.T) {
		mfaTokenSpec := newMfaTokenSpec(&Config{Host: "host.com", User: "johndoe"})
		mfaCred := "token12"
		idTokenSpec := newIDTokenSpec(&Config{Host: "host.com", User: "johndoe"})
		idCred := "token34"
		idTokenSpec2 := newIDTokenSpec(&Config{Host: "host.org", User: "johndoe"})
		idCred2 := "token56"
		idTokenSpec3 := newIDTokenSpec(&Config{Host: "host.com", User: "someoneelse"})
		idCred3 := "token78"
		ssm.setCredential(mfaTokenSpec, mfaCred)
		ssm.setCredential(idTokenSpec, idCred)
		ssm.setCredential(idTokenSpec2, idCred2)
		ssm.setCredential(idTokenSpec3, idCred3)
		assertEqualE(t, ssm.getCredential(mfaTokenSpec), mfaCred)
		assertEqualE(t, ssm.getCredential(idTokenSpec), idCred)
		assertEqualE(t, ssm.getCredential(idTokenSpec2), idCred2)
		assertEqualE(t, ssm.getCredential(idTokenSpec3), idCred3)
		ssm.deleteCredential(mfaTokenSpec)
		assertEqualE(t, ssm.getCredential(mfaTokenSpec), "")
		assertEqualE(t, ssm.getCredential(idTokenSpec), idCred)
		assertEqualE(t, ssm.getCredential(idTokenSpec2), idCred2)
		assertEqualE(t, ssm.getCredential(idTokenSpec3), idCred3)
	})

	t.Run("override single token", func(t *testing.T) {
		mfaTokenSpec := newMfaTokenSpec(&Config{Host: "host.com", User: "johndoe"})
		mfaCred := "token123"
		idTokenSpec := newIDTokenSpec(&Config{Host: "host.com", User: "johndoe"})
		idCred := "token456"
		ssm.setCredential(mfaTokenSpec, mfaCred)
		ssm.setCredential(idTokenSpec, idCred)
		assertEqualE(t, ssm.getCredential(mfaTokenSpec), mfaCred)
		mfaCredOverride := "token789"
		ssm.setCredential(mfaTokenSpec, mfaCredOverride)
		assertEqualE(t, ssm.getCredential(mfaTokenSpec), mfaCredOverride)
		ssm.setCredential(idTokenSpec, idCred)
	})

	t.Run("reuses a leftover lock file", func(t *testing.T) {
		tokenSpec := newMfaTokenSpec(&Config{Host: "stale", User: "cache"})
		leftover, err := os.OpenFile(ssm.lockPath, os.O_RDWR|os.O_CREATE, 0600)
		assertNilF(t, err)
		assertNilF(t, leftover.Close())
		ssm.setCredential(tokenSpec, "unlocked")
		assertEqualE(t, ssm.getCredential(tokenSpec), "unlocked")
		lockInfo, err := os.Stat(ssm.lockPath)
		assertNilF(t, err)
		assertTrueE(t, lockInfo.Mode().IsRegular())
		assertFalseE(t, lockInfo.IsDir())
	})

	t.Run("wait for other process to unlock cache", func(t *testing.T) {
		tokenSpec := newMfaTokenSpec(&Config{Host: "stale", User: "cache"})
		lockFile, err := os.OpenFile(ssm.lockPath, os.O_RDWR|os.O_CREATE, 0600)
		assertNilF(t, err)
		defer lockFile.Close()
		assertNilF(t, tryFlockExclusive(lockFile))

		setDone := make(chan struct{})
		go func() {
			ssm.setCredential(tokenSpec, "unlocked")
			close(setDone)
		}()

		select {
		case <-setDone:
			t.Fatal("setCredential completed while lock was held")
		case <-time.After(150 * time.Millisecond):
		}

		assertNilF(t, unlockFlock(lockFile))
		select {
		case <-setDone:
		case <-time.After(5 * time.Second):
			t.Fatal("setCredential did not complete after unlock")
		}
		assertEqualE(t, ssm.getCredential(tokenSpec), "unlocked")
		lockInfo, err := os.Stat(ssm.lockPath)
		assertNilF(t, err)
		assertTrueE(t, lockInfo.Mode().IsRegular())
	})

	t.Run("concurrent writers serialize", func(t *testing.T) {
		specA := newMfaTokenSpec(&Config{Host: "concurrent-a", User: "cache"})
		specB := newMfaTokenSpec(&Config{Host: "concurrent-b", User: "cache"})
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			ssm.setCredential(specA, "token-a")
		}()
		go func() {
			defer wg.Done()
			<-start
			ssm.setCredential(specB, "token-b")
		}()
		close(start)
		wg.Wait()
		assertEqualE(t, ssm.getCredential(specA), "token-a")
		assertEqualE(t, ssm.getCredential(specB), "token-b")
	})

	t.Run("write path replaces corrupt cache file", func(t *testing.T) {
		lostSpec := newMfaTokenSpec(&Config{Host: "corrupt-write-lost", User: "cache"})
		tokenSpec := newMfaTokenSpec(&Config{Host: "corrupt-write", User: "cache"})
		ssm.setCredential(lostSpec, "will-be-lost")
		assertNilF(t, os.WriteFile(ssm.credFilePath, []byte("{not json"), 0600))
		ssm.setCredential(tokenSpec, "recovered")
		assertEqualE(t, ssm.getCredential(tokenSpec), "recovered")
		assertEqualE(t, ssm.getCredential(lostSpec), "")
		fileContent, err := os.ReadFile(ssm.credFilePath)
		assertNilF(t, err)
		var m map[string]any
		assertNilF(t, json.Unmarshal(fileContent, &m))
		cacheKey, err := tokenSpec.buildKey()
		assertNilF(t, err)
		assertEqualE(t, ssm.getTokens(m)[cacheKey], "recovered")
	})

	t.Run("read path treats corrupt cache as miss", func(t *testing.T) {
		tokenSpec := newMfaTokenSpec(&Config{Host: "corrupt-read", User: "cache"})
		assertNilF(t, os.WriteFile(ssm.credFilePath, []byte("{not json"), 0600))
		assertEqualE(t, ssm.getCredential(tokenSpec), "")
		fileContent, err := os.ReadFile(ssm.credFilePath)
		assertNilF(t, err)
		assertEqualE(t, string(fileContent), "{not json")
	})

	t.Run("should not modify keys other than tokens", func(t *testing.T) {
		content := []byte(`{
			"otherKey": "otherValue"
		}`)
		err = os.WriteFile(ssm.credFilePath, content, 0600)
		assertNilF(t, err)
		ssm.setCredential(newMfaTokenSpec(&Config{Host: "somehost.com", User: "someUser"}), "someToken")
		result, err := os.ReadFile(ssm.credFilePath)
		assertNilF(t, err)
		assertStringContainsE(t, string(result), `"otherKey":"otherValue"`)
	})

	t.Run("should not modify file if it has wrong permission", func(t *testing.T) {
		tokenSpec := newMfaTokenSpec(&Config{Host: "somehost.com", User: "someUser"})
		ssm.setCredential(tokenSpec, "initialValue")
		assertEqualE(t, ssm.getCredential(tokenSpec), "initialValue")
		err = os.Chmod(ssm.credFilePath, 0644)
		assertNilF(t, err)
		defer func() {
			assertNilE(t, os.Chmod(ssm.credFilePath, 0600))
		}()
		ssm.setCredential(tokenSpec, "newValue")
		assertEqualE(t, ssm.getCredential(tokenSpec), "")
		fileContent, err := os.ReadFile(ssm.credFilePath)
		assertNilF(t, err)
		var m map[string]any
		err = json.Unmarshal(fileContent, &m)
		assertNilF(t, err)
		cacheKey, err := tokenSpec.buildKey()
		assertNilF(t, err)
		tokens := m["tokens"].(map[string]any)
		assertEqualE(t, tokens[cacheKey], "initialValue")
	})

	t.Run("should not modify file if its dir has wrong permission", func(t *testing.T) {
		tokenSpec := newMfaTokenSpec(&Config{Host: "somehost.com", User: "someUser"})
		ssm.setCredential(tokenSpec, "initialValue")
		assertEqualE(t, ssm.getCredential(tokenSpec), "initialValue")
		err = os.Chmod(ssm.credDirPath, 0777)
		assertNilF(t, err)
		defer func() {
			assertNilE(t, os.Chmod(ssm.credDirPath, 0700))
		}()
		ssm.setCredential(tokenSpec, "newValue")
		assertEqualE(t, ssm.getCredential(tokenSpec), "")
		fileContent, err := os.ReadFile(ssm.credFilePath)
		assertNilF(t, err)
		var m map[string]any
		err = json.Unmarshal(fileContent, &m)
		assertNilF(t, err)
		cacheKey, err := tokenSpec.buildKey()
		assertNilF(t, err)
		tokens := m["tokens"].(map[string]any)
		assertEqualE(t, tokens[cacheKey], "initialValue")
	})
}

func TestFileBasedCacheSkipPermissionsVerification(t *testing.T) {
	skipOnWindows(t, "file system permission is different")
	credCacheDir, err := os.MkdirTemp("", "")
	assertNilF(t, err)
	defer os.RemoveAll(credCacheDir)
	credCacheDirEnvOverride := overrideEnv(credCacheDirEnv, credCacheDir)
	defer credCacheDirEnvOverride.rollback()

	skipEnvOverride := overrideEnv(sfconfig.SkipTokenFilePermissionsVerificationEnv, "true")
	defer skipEnvOverride.rollback()

	ssm, err := newFileBasedSecureStorageManager()
	assertNilF(t, err)

	tokenSpec := newMfaTokenSpec(&Config{Host: "somehost.com", User: "someUser"})

	t.Run("round-trip with 0644 cache file", func(t *testing.T) {
		ssm.setCredential(tokenSpec, "initialValue")
		assertEqualE(t, ssm.getCredential(tokenSpec), "initialValue")
		assertNilF(t, os.Chmod(ssm.credFilePath, 0644))
		defer func() {
			assertNilE(t, os.Chmod(ssm.credFilePath, 0600))
		}()
		ssm.setCredential(tokenSpec, "newValue")
		assertEqualE(t, ssm.getCredential(tokenSpec), "newValue")
	})

	t.Run("round-trip with 0777 cache dir", func(t *testing.T) {
		ssm.setCredential(tokenSpec, "initialValue")
		assertEqualE(t, ssm.getCredential(tokenSpec), "initialValue")
		assertNilF(t, os.Chmod(ssm.credDirPath, 0777))
		defer func() {
			assertNilE(t, os.Chmod(ssm.credDirPath, 0700))
		}()
		ssm.setCredential(tokenSpec, "newValue")
		assertEqualE(t, ssm.getCredential(tokenSpec), "newValue")
	})
}

func TestSetAndGetCredential(t *testing.T) {
	skipOnMissingHome(t)
	for _, tc := range []struct {
		name string
		spec secureTokenSpec
	}{
		{string(mfaToken), newMfaTokenSpec(&Config{Host: "testhost", User: "testuser"})},
		{string(idToken), newIDTokenSpec(&Config{Host: "testhost", User: "testuser"})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			skipOnMac(t, "keyring asks for password")
			fakeMfaToken := "test token"
			credentialsStorage.setCredential(tc.spec, fakeMfaToken)
			assertEqualE(t, credentialsStorage.getCredential(tc.spec), fakeMfaToken)

			credentialsStorage.deleteCredential(tc.spec)
			assertEqualE(t, credentialsStorage.getCredential(tc.spec), "")
		})
	}
}

func TestSkipStoringCredentialIfUserIsEmpty(t *testing.T) {
	specs := []struct {
		host string
		spec secureTokenSpec
	}{
		{"mfaHost.com", newMfaTokenSpec(&Config{Host: "mfaHost.com", User: ""})},
		{"idHost.com", newIDTokenSpec(&Config{Host: "idHost.com", User: ""})},
	}

	for _, tc := range specs {
		t.Run(tc.host, func(t *testing.T) {
			credentialsStorage.setCredential(tc.spec, "non-empty-value")
			assertEqualE(t, credentialsStorage.getCredential(tc.spec), "")
		})
	}
}

func TestSkipStoringCredentialIfHostIsEmpty(t *testing.T) {
	specs := []struct {
		user string
		spec secureTokenSpec
	}{
		{"mfaUser", newMfaTokenSpec(&Config{Host: "", User: "mfaUser"})},
		{"idUser", newIDTokenSpec(&Config{Host: "", User: "idUser"})},
	}

	for _, tc := range specs {
		t.Run(tc.user, func(t *testing.T) {
			credentialsStorage.setCredential(tc.spec, "non-empty-value")
			assertEqualE(t, credentialsStorage.getCredential(tc.spec), "")
		})
	}
}

func TestStoreTemporaryCredential(t *testing.T) {
	if runningOnGithubAction() {
		t.Skip("cannot write to github file system")
	}

	testcases := []struct {
		tokenSpec secureTokenSpec
		value     string
	}{
		{newMfaTokenSpec(&Config{Host: "testhost", User: "testuser"}), "mfa token"},
		{newIDTokenSpec(&Config{Host: "testhost", User: "testuser"}), "id token"},
		{newOAuthAccessTokenSpec(&Config{OauthTokenRequestURL: "https://idp.example.com/token", Host: "testhost", User: "testuser", Role: "testrole"}), "access token"},
		{newOAuthRefreshTokenSpec(&Config{OauthTokenRequestURL: "https://idp.example.com/token", Host: "testhost", User: "testuser", Role: "testrole"}), "refresh token"},
	}

	credCacheDir, err := os.MkdirTemp("", "")
	assertNilF(t, err)
	defer os.RemoveAll(credCacheDir)
	credCacheDirEnvOverride := overrideEnv(credCacheDirEnv, credCacheDir)
	defer credCacheDirEnvOverride.rollback()

	ssm, err := newFileBasedSecureStorageManager()
	assertNilF(t, err)

	for _, test := range testcases {
		t.Run(test.value, func(t *testing.T) {
			ssm.setCredential(test.tokenSpec, test.value)
			assertEqualE(t, ssm.getCredential(test.tokenSpec), test.value)
			ssm.deleteCredential(test.tokenSpec)
			assertEqualE(t, ssm.getCredential(test.tokenSpec), "")
		})
	}
}

func TestBuildCacheKeyOAuthGoldenHash(t *testing.T) {
	// Golden vector A: OAuth flow. DpopBundledAccessToken is a test literal;
	// this driver has no DPoP flow.
	// Expected canonical JSON (4 OAuth fields, no token_type):
	//   {"idp":"login.microsoftonline.com:443/tenant-id/oauth2/v2.0",
	//    "role":"\"Analyst Role With Spaces\":north_america:prod:readonly",
	//    "snowflake":"myorg-myaccount.privatelink.snowflakecomputing.com",
	//    "username":"\"First Last\"@long-corporate-domain.example.com"}
	spec := &oauthTokenSpec{
		tokenType: "DpopBundledAccessToken",
		idp:       "https://login.microsoftonline.com:443/tenant-id/oauth2/v2.0",
		snowflake: "https://myorg-myaccount.privatelink.snowflakecomputing.com",
		username:  `"First Last"@long-corporate-domain.example.com`,
		role:      `"Analyst Role With Spaces":north_america:prod:readonly`,
	}
	key, err := spec.buildKey()
	assertNilF(t, err)
	assertEqualF(t, key, "SnowflakeTokenCache.v2.DpopBundledAccessToken.741b6d66d252666d6821bfd19e0151511cf4efdaaeba2b3c87673aa4de6d2c0b") // pragma: allowlist secret
}

func TestBuildCacheKeyMfaGoldenHash(t *testing.T) {
	// Golden vector B: MFA flow — only snowflake + username in keyData.
	// Expected canonical JSON (2 MFA fields, no idp/role/token_type):
	//   {"snowflake":"myorg-myaccount.privatelink.snowflakecomputing.com",
	//    "username":"\"First Last\"@long-corporate-domain.example.com"}
	spec := &hostUserTokenSpec{
		tokenType: mfaToken,
		snowflake: "https://myorg-myaccount.privatelink.snowflakecomputing.com",
		username:  `"First Last"@long-corporate-domain.example.com`,
	}
	key, err := spec.buildKey()
	assertNilF(t, err)
	assertEqualF(t, key, "SnowflakeTokenCache.v2.MfaToken.10c5dde84bb8f584c0df06ea826d418c4f580e08f9db10187c0cb5e2a732a0d6") // pragma: allowlist secret
}

func TestBuildCacheKeyValidation(t *testing.T) {
	t.Run("empty snowflake rejects", func(t *testing.T) {
		spec := &hostUserTokenSpec{tokenType: mfaToken, snowflake: "", username: "user"}
		_, err := spec.buildKey()
		assertNotNilF(t, err)
		assertStringContainsE(t, err.Error(), "snowflake URL is required")
	})

	t.Run("empty username rejects", func(t *testing.T) {
		spec := &hostUserTokenSpec{tokenType: mfaToken, snowflake: "host.com", username: ""}
		_, err := spec.buildKey()
		assertNotNilF(t, err)
		assertStringContainsE(t, err.Error(), "username is required")
	})

	t.Run("OAuth empty idp rejects", func(t *testing.T) {
		spec := &oauthTokenSpec{tokenType: oauthAccessToken, idp: "", snowflake: "host.com", username: "user"}
		_, err := spec.buildKey()
		assertNotNilF(t, err)
		assertStringContainsE(t, err.Error(), "idp URL is required")
	})

	t.Run("scheme-only snowflake rejects after normalize", func(t *testing.T) {
		spec := &hostUserTokenSpec{tokenType: mfaToken, snowflake: "https://", username: "user"}
		_, err := spec.buildKey()
		assertNotNilF(t, err)
		assertStringContainsE(t, err.Error(), "snowflake URL is required")
	})

	t.Run("OAuth scheme-only idp rejects after normalize", func(t *testing.T) {
		spec := &oauthTokenSpec{tokenType: oauthAccessToken, idp: "https://", snowflake: "host.com", username: "user"}
		_, err := spec.buildKey()
		assertNotNilF(t, err)
		assertStringContainsE(t, err.Error(), "idp URL is required")
	})
}

func TestNormalizeURL(t *testing.T) {
	testcases := []struct {
		input    string
		expected string
	}{
		{"https://login.microsoftonline.com:443/tenant-id/oauth2/v2.0", "login.microsoftonline.com:443/tenant-id/oauth2/v2.0"},
		{"https://myorg-myaccount.privatelink.snowflakecomputing.com", "myorg-myaccount.privatelink.snowflakecomputing.com"},
		{"http://example.com/", "example.com"},
		{"https://user:pass@host.com/path", "host.com/path"},
		{"host.com", "host.com"},
		{"https://host.com?query=1#frag", "host.com"},
		{"https://host.com?user=@evil.com", "host.com"},
		{"https://host.com?user=a@b/c", "host.com"},
		{"https://host.com#@evil.com", "host.com"},
		{"https://idp.example.com/oauth/token/", "idp.example.com/oauth/token"},
		{"https://host.example.com/oauth/@handle/token", "host.example.com/oauth/@handle/token"},
		{"https://user:pass@host.example.com/oauth/@handle/token", "host.example.com/oauth/@handle/token"},
		{"HTTPS://Login.MicrosoftOnline.COM:8443/Tenant-ID/OAuth2/V2.0", "login.microsoftonline.com:8443/tenant-id/oauth2/v2.0"},
		{"https://HOST.EXAMPLE.COM/OAuth/TOKEN/", "host.example.com/oauth/token"},
		{"https://host.example.com:443/oauth/token", "host.example.com:443/oauth/token"},
		{"https://host.example.com/oauth/token", "host.example.com/oauth/token"},
		{"", ""},
	}
	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			assertEqualE(t, normalizeURL(tc.input), tc.expected)
		})
	}
}

func TestNormalizeIdentifier(t *testing.T) {
	testcases := []struct {
		input    string
		expected string
	}{
		{`"First Last"@long-corporate-domain.example.com`, `"First Last"@long-corporate-domain.example.com`},
		{`"Analyst Role With Spaces":north_america:prod:readonly`, `"Analyst Role With Spaces":north_america:prod:readonly`},
		{"simpleRole", "simplerole"},
		{`"CaseSensitive"`, `"CaseSensitive"`},
		{`"lower inside"@Mixed.OUTSIDE`, `"lower inside"@Mixed.OUTSIDE`},
		{`"First Last"@domain.com`, `"First Last"@domain.com`},
		{"USER@DOMAIN.COM", "user@domain.com"},
		{"SimpleRole", "simplerole"},
		{"", ""},
	}
	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			assertEqualE(t, normalizeIdentifier(tc.input), tc.expected)
		})
	}
}

func TestDifferentSnowflakeHostsProduceDifferentKeys(t *testing.T) {
	key1, err := (&oauthTokenSpec{
		tokenType: oauthAccessToken,
		idp:       "https://idp.example.com/token",
		snowflake: "https://account1.snowflakecomputing.com",
		username:  "user",
		role:      "ANALYST",
	}).buildKey()
	assertNilF(t, err)

	key2, err := (&oauthTokenSpec{
		tokenType: oauthAccessToken,
		idp:       "https://idp.example.com/token",
		snowflake: "https://account2.snowflakecomputing.com",
		username:  "user",
		role:      "ANALYST",
	}).buildKey()
	assertNilF(t, err)

	assertNotEqualF(t, key1, key2)
}

func TestDifferentRolesProduceDifferentKeys(t *testing.T) {
	key1, err := (&oauthTokenSpec{
		tokenType: oauthAccessToken,
		idp:       "https://idp.example.com/token",
		snowflake: "https://account.snowflakecomputing.com",
		username:  "user",
		role:      "ANALYST",
	}).buildKey()
	assertNilF(t, err)

	key2, err := (&oauthTokenSpec{
		tokenType: oauthAccessToken,
		idp:       "https://idp.example.com/token",
		snowflake: "https://account.snowflakecomputing.com",
		username:  "user",
		role:      "ADMIN",
	}).buildKey()
	assertNilF(t, err)

	assertNotEqualF(t, key1, key2)
}

func TestMfaWithEmptyRoleProducesStableKey(t *testing.T) {
	key1, err := (&hostUserTokenSpec{
		tokenType: mfaToken,
		snowflake: "https://account.snowflakecomputing.com",
		username:  "user",
	}).buildKey()
	assertNilF(t, err)

	key2, err := (&hostUserTokenSpec{
		tokenType: mfaToken,
		snowflake: "https://account.snowflakecomputing.com",
		username:  "user",
	}).buildKey()
	assertNilF(t, err)

	assertEqualF(t, key1, key2)
	assertStringContainsE(t, key1, "SnowflakeTokenCache.v2.MfaToken.")
}

func TestDifferentTokenTypesProduceDifferentKeys(t *testing.T) {
	t.Run("OAuth access vs refresh", func(t *testing.T) {
		key1, err := (&oauthTokenSpec{
			tokenType: oauthAccessToken,
			idp:       "https://idp.example.com/token",
			snowflake: "https://account.snowflakecomputing.com",
			username:  "user",
			role:      "ANALYST",
		}).buildKey()
		assertNilF(t, err)

		key2, err := (&oauthTokenSpec{
			tokenType: oauthRefreshToken,
			idp:       "https://idp.example.com/token",
			snowflake: "https://account.snowflakecomputing.com",
			username:  "user",
			role:      "ANALYST",
		}).buildKey()
		assertNilF(t, err)
		assertNotEqualF(t, key1, key2)
	})

	t.Run("MFA vs OAuth for same user/host", func(t *testing.T) {
		mfaKey, err := (&hostUserTokenSpec{
			tokenType: mfaToken,
			snowflake: "https://account.snowflakecomputing.com",
			username:  "user",
		}).buildKey()
		assertNilF(t, err)

		oauthKey, err := (&oauthTokenSpec{
			tokenType: oauthAccessToken,
			idp:       "https://account.snowflakecomputing.com",
			snowflake: "https://account.snowflakecomputing.com",
			username:  "user",
			role:      "",
		}).buildKey()
		assertNilF(t, err)
		assertNotEqualF(t, mfaKey, oauthKey)
	})
}

func TestCacheKeyUsesV2Prefix(t *testing.T) {
	spec := &hostUserTokenSpec{tokenType: mfaToken, snowflake: "host.com", username: "user"}
	key, err := spec.buildKey()
	assertNilF(t, err)
	assertStringContainsE(t, key, "SnowflakeTokenCache.v2.MfaToken.")
}

func TestFileBackendStoresKeyVerbatim(t *testing.T) {
	skipOnWindows(t, "permission model is different")
	credCacheDir, err := os.MkdirTemp("", "")
	assertNilF(t, err)
	defer os.RemoveAll(credCacheDir)
	credCacheDirEnvOverride := overrideEnv(credCacheDirEnv, credCacheDir)
	defer credCacheDirEnvOverride.rollback()
	ssm, err := newFileBasedSecureStorageManager()
	assertNilF(t, err)

	tokenSpec := newMfaTokenSpec(&Config{Host: "host.com", User: "testuser"})
	ssm.setCredential(tokenSpec, "testvalue")

	fileContent, err := os.ReadFile(ssm.credFilePath)
	assertNilF(t, err)
	var m map[string]any
	err = json.Unmarshal(fileContent, &m)
	assertNilF(t, err)

	expectedKey, err := tokenSpec.buildKey()
	assertNilF(t, err)
	assertStringContainsE(t, expectedKey, "SnowflakeTokenCache.v2.MfaToken.")

	tokens := m["tokens"].(map[string]any)
	assertEqualE(t, tokens[expectedKey], "testvalue")
}

func TestMfaKeyHasNoIdpOrRole(t *testing.T) {
	expected2FieldJSON := `{"snowflake":"host.com","username":"user"}`
	sum := sha256.Sum256([]byte(expected2FieldJSON))
	expectedHash := hex.EncodeToString(sum[:])

	spec := &hostUserTokenSpec{tokenType: mfaToken, snowflake: "host.com", username: "user"}
	key, err := spec.buildKey()
	assertNilF(t, err)
	assertEqualF(t, key, "SnowflakeTokenCache.v2.MfaToken."+expectedHash)
}

func TestOAuthKeyHasFourFields(t *testing.T) {
	expected4FieldJSON := `{"idp":"idp.com","role":"analyst","snowflake":"host.com","username":"user"}`
	sum := sha256.Sum256([]byte(expected4FieldJSON))
	expectedHash := hex.EncodeToString(sum[:])

	spec := &oauthTokenSpec{
		tokenType: oauthAccessToken,
		idp:       "idp.com",
		snowflake: "host.com",
		username:  "user",
		role:      "analyst",
	}
	key, err := spec.buildKey()
	assertNilF(t, err)
	assertEqualF(t, key, "SnowflakeTokenCache.v2.OauthAccessToken."+expectedHash)
}

func TestCacheKeyDimensionIsolation(t *testing.T) {
	baseOAuth := oauthTokenSpec{
		tokenType: oauthAccessToken,
		idp:       "https://idp.example.com/token",
		snowflake: "https://account1.snowflakecomputing.com",
		username:  "user",
		role:      "analyst",
	}
	baseKey, err := baseOAuth.buildKey()
	assertNilF(t, err)

	otherUser := baseOAuth
	otherUser.username = "other-user"
	keyUser, err := otherUser.buildKey()
	assertNilF(t, err)
	assertNotEqualF(t, baseKey, keyUser)

	otherIdp := baseOAuth
	otherIdp.idp = "https://other-idp.example.com/token"
	keyIdp, err := otherIdp.buildKey()
	assertNilF(t, err)
	assertNotEqualF(t, baseKey, keyIdp)

	mfaKey, err := (&hostUserTokenSpec{
		tokenType: mfaToken,
		snowflake: baseOAuth.snowflake,
		username:  baseOAuth.username,
	}).buildKey()
	assertNilF(t, err)
	assertStringContainsE(t, mfaKey, "SnowflakeTokenCache.v2.MfaToken.")

	idKey, err := (&hostUserTokenSpec{
		tokenType: idToken,
		snowflake: baseOAuth.snowflake,
		username:  baseOAuth.username,
	}).buildKey()
	assertNilF(t, err)
	assertNotEqualF(t, mfaKey, idKey)
	assertStringContainsE(t, idKey, "SnowflakeTokenCache.v2.IdToken.")
}

func expectedCacheKey(tokenTypeName, canonicalJSON string) string {
	sum := sha256.Sum256([]byte(canonicalJSON))
	return "SnowflakeTokenCache.v2." + tokenTypeName + "." + hex.EncodeToString(sum[:])
}

func TestBuildCacheKeyFromConfigDefaultTokenURL(t *testing.T) {
	cfg := &Config{
		Protocol: "https",
		Host:     "account.snowflakecomputing.com",
		Port:     443,
		User:     "testuser",
		Role:     "analyst",
	}

	oauthKey, err := newOAuthAccessTokenSpec(cfg).buildKey()
	assertNilF(t, err)
	assertEqualF(t, oauthKey, expectedCacheKey("OauthAccessToken",
		`{"idp":"account.snowflakecomputing.com/oauth/token-request","role":"analyst","snowflake":"account.snowflakecomputing.com","username":"testuser"}`))

	mfaKey, err := newMfaTokenSpec(cfg).buildKey()
	assertNilF(t, err)
	assertEqualF(t, mfaKey, expectedCacheKey("MfaToken",
		`{"snowflake":"account.snowflakecomputing.com","username":"testuser"}`))

	cfg.Port = 0
	oauthKeyNoPort, err := newOAuthAccessTokenSpec(cfg).buildKey()
	assertNilF(t, err)
	assertEqualF(t, oauthKeyNoPort, oauthKey)
}

func TestBuildCacheKeyFromConfigExplicitPort(t *testing.T) {
	cfg := &Config{
		Protocol: "https",
		Host:     "account.snowflakecomputing.com",
		Port:     8443,
		User:     "testuser",
		Role:     "analyst",
	}

	oauthKey, err := newOAuthAccessTokenSpec(cfg).buildKey()
	assertNilF(t, err)
	assertEqualF(t, oauthKey, expectedCacheKey("OauthAccessToken",
		`{"idp":"account.snowflakecomputing.com:8443/oauth/token-request","role":"analyst","snowflake":"account.snowflakecomputing.com:8443","username":"testuser"}`))

	mfaKey, err := newMfaTokenSpec(cfg).buildKey()
	assertNilF(t, err)
	assertEqualF(t, mfaKey, expectedCacheKey("MfaToken",
		`{"snowflake":"account.snowflakecomputing.com:8443","username":"testuser"}`))
}

func TestHostWithOptionalPort(t *testing.T) {
	for _, tc := range []struct {
		cfg      *Config
		expected string
	}{
		{&Config{Host: "account.snowflakecomputing.com", Port: 443}, "account.snowflakecomputing.com"},
		{&Config{Host: "account.snowflakecomputing.com", Port: 8443}, "account.snowflakecomputing.com:8443"},
		{&Config{Host: "account.snowflakecomputing.com", Protocol: "http", Port: 80}, "account.snowflakecomputing.com"},
		{&Config{Host: "::1", Port: 8443}, "[::1]:8443"},
	} {
		t.Run(tc.expected, func(t *testing.T) {
			assertEqualE(t, hostWithOptionalPort(tc.cfg), tc.expected)
		})
	}
}

func TestCacheKeyDoesNotHTMLEscape(t *testing.T) {
	spec := &oauthTokenSpec{
		tokenType: oauthAccessToken,
		idp:       "https://idp.example.com/token",
		snowflake: "https://account.snowflakecomputing.com",
		username:  "user",
		role:      `"R&D<prod>"`,
	}
	key, err := spec.buildKey()
	assertNilF(t, err)
	assertEqualF(t, key, expectedCacheKey("OauthAccessToken",
		`{"idp":"idp.example.com/token","role":"\"R&D<prod>\"","snowflake":"account.snowflakecomputing.com","username":"user"}`))
}

func TestLockIDUsesBuiltCacheKey(t *testing.T) {
	upper := newMfaTokenSpec(&Config{Host: "host.com", User: "USER"})
	lower := newMfaTokenSpec(&Config{Host: "host.com", User: "user"})
	upperKey, err := upper.buildKey()
	assertNilF(t, err)
	assertEqualE(t, upper.lockID(), upperKey)
	assertEqualE(t, upper.lockID(), lower.lockID())

	oauthUpper := newOAuthAccessTokenSpec(&Config{
		OauthTokenRequestURL: "https://idp.example.com/token",
		Host:                 "host.com",
		User:                 "USER",
		Role:                 "ANALYST",
	})
	oauthLower := newOAuthAccessTokenSpec(&Config{
		OauthTokenRequestURL: "https://idp.example.com/token",
		Host:                 "host.com",
		User:                 "user",
		Role:                 "analyst",
	})
	oauthKey, err := oauthUpper.buildKey()
	assertNilF(t, err)
	assertEqualE(t, oauthUpper.lockID(), oauthKey)
	assertEqualE(t, oauthUpper.lockID(), oauthLower.lockID())

	invalid := newMfaTokenSpec(&Config{Host: "host.com", User: ""})
	assertEqualE(t, invalid.lockID(), "host.com||MfaToken")
}

func TestLookupCacheDirRelativePathDoesNotPanic(t *testing.T) {
	skipOnWindows(t, "permission model is different")
	t.Chdir(t.TempDir())
	env := overrideEnv("CACHE_DIR_REL", ".")
	defer env.rollback()
	path, err := lookupCacheDir("CACHE_DIR_REL")
	assertNilF(t, err)
	assertEqualE(t, path, ".")
}
