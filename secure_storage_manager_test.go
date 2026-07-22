package gosnowflake

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
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

func TestSnowflakeFileBasedSecureStorageManager(t *testing.T) {
	skipOnWindows(t, "file system permission is different")
	credCacheDir, err := os.MkdirTemp("", "")
	assertNilF(t, err)
	assertNilF(t, os.MkdirAll(credCacheDir, os.ModePerm))
	credCacheDirEnvOverride := overrideEnv(credCacheDirEnv, credCacheDir)
	defer credCacheDirEnvOverride.rollback()
	ssm, err := newFileBasedSecureStorageManager()
	assertNilF(t, err)

	t.Run("store single token", func(t *testing.T) {
		tokenSpec := newMfaTokenSpec("host.com", "johndoe")
		cred := "token123"
		ssm.setCredential(tokenSpec, cred)
		assertEqualE(t, ssm.getCredential(tokenSpec), cred)
		ssm.deleteCredential(tokenSpec)
		assertEqualE(t, ssm.getCredential(tokenSpec), "")
	})

	t.Run("store tokens of different types, hosts and users", func(t *testing.T) {
		mfaTokenSpec := newMfaTokenSpec("host.com", "johndoe")
		mfaCred := "token12"
		idTokenSpec := newIDTokenSpec("host.com", "johndoe")
		idCred := "token34"
		idTokenSpec2 := newIDTokenSpec("host.org", "johndoe")
		idCred2 := "token56"
		idTokenSpec3 := newIDTokenSpec("host.com", "someoneelse")
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
		mfaTokenSpec := newMfaTokenSpec("host.com", "johndoe")
		mfaCred := "token123"
		idTokenSpec := newIDTokenSpec("host.com", "johndoe")
		idCred := "token456"
		ssm.setCredential(mfaTokenSpec, mfaCred)
		ssm.setCredential(idTokenSpec, idCred)
		assertEqualE(t, ssm.getCredential(mfaTokenSpec), mfaCred)
		mfaCredOverride := "token789"
		ssm.setCredential(mfaTokenSpec, mfaCredOverride)
		assertEqualE(t, ssm.getCredential(mfaTokenSpec), mfaCredOverride)
		ssm.setCredential(idTokenSpec, idCred)
	})

	t.Run("unlock stale cache", func(t *testing.T) {
		tokenSpec := newMfaTokenSpec("stale", "cache")
		assertNilF(t, os.Mkdir(ssm.lockPath(), 0700))
		time.Sleep(1000 * time.Millisecond)
		ssm.setCredential(tokenSpec, "unlocked")
		assertEqualE(t, ssm.getCredential(tokenSpec), "unlocked")
	})

	t.Run("wait for other process to unlock cache", func(t *testing.T) {
		tokenSpec := newMfaTokenSpec("stale", "cache")
		startTime := time.Now()
		assertNilF(t, os.Mkdir(ssm.lockPath(), 0700))
		time.Sleep(500 * time.Millisecond)
		go func() {
			time.Sleep(500 * time.Millisecond)
			assertNilF(t, os.Remove(ssm.lockPath()))
		}()
		ssm.setCredential(tokenSpec, "unlocked")
		totalDurationMillis := time.Since(startTime).Milliseconds()
		assertEqualE(t, ssm.getCredential(tokenSpec), "unlocked")
		assertTrueE(t, totalDurationMillis > 1000 && totalDurationMillis < 1200)
	})

	t.Run("should not modify keys other than tokens", func(t *testing.T) {
		content := []byte(`{
			"otherKey": "otherValue"
		}`)
		err = os.WriteFile(ssm.credFilePath(), content, 0600)
		assertNilF(t, err)
		ssm.setCredential(newMfaTokenSpec("somehost.com", "someUser"), "someToken")
		result, err := os.ReadFile(ssm.credFilePath())
		assertNilF(t, err)
		assertStringContainsE(t, string(result), `"otherKey":"otherValue"`)
	})

	t.Run("should not modify file if it has wrong permission", func(t *testing.T) {
		tokenSpec := newMfaTokenSpec("somehost.com", "someUser")
		ssm.setCredential(tokenSpec, "initialValue")
		assertEqualE(t, ssm.getCredential(tokenSpec), "initialValue")
		err = os.Chmod(ssm.credFilePath(), 0644)
		assertNilF(t, err)
		defer func() {
			assertNilE(t, os.Chmod(ssm.credFilePath(), 0600))
		}()
		ssm.setCredential(tokenSpec, "newValue")
		assertEqualE(t, ssm.getCredential(tokenSpec), "")
		fileContent, err := os.ReadFile(ssm.credFilePath())
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
		tokenSpec := newMfaTokenSpec("somehost.com", "someUser")
		ssm.setCredential(tokenSpec, "initialValue")
		assertEqualE(t, ssm.getCredential(tokenSpec), "initialValue")
		err = os.Chmod(ssm.credDirPath, 0777)
		assertNilF(t, err)
		defer func() {
			assertNilE(t, os.Chmod(ssm.credDirPath, 0700))
		}()
		ssm.setCredential(tokenSpec, "newValue")
		assertEqualE(t, ssm.getCredential(tokenSpec), "")
		fileContent, err := os.ReadFile(ssm.credFilePath())
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

	tokenSpec := newMfaTokenSpec("somehost.com", "someUser")

	t.Run("round-trip with 0644 cache file", func(t *testing.T) {
		ssm.setCredential(tokenSpec, "initialValue")
		assertEqualE(t, ssm.getCredential(tokenSpec), "initialValue")
		assertNilF(t, os.Chmod(ssm.credFilePath(), 0644))
		defer func() {
			assertNilE(t, os.Chmod(ssm.credFilePath(), 0600))
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
	for _, tokenSpec := range []*secureTokenSpec{
		newMfaTokenSpec("testhost", "testuser"),
		newIDTokenSpec("testhost", "testuser"),
	} {
		t.Run(string(tokenSpec.input.tokenType), func(t *testing.T) {
			skipOnMac(t, "keyring asks for password")
			fakeMfaToken := "test token"
			tokenSpec := newMfaTokenSpec("testHost", "testUser")
			credentialsStorage.setCredential(tokenSpec, fakeMfaToken)
			assertEqualE(t, credentialsStorage.getCredential(tokenSpec), fakeMfaToken)

			credentialsStorage.deleteCredential(tokenSpec)
			assertEqualE(t, credentialsStorage.getCredential(tokenSpec), "")
		})
	}
}

func TestSkipStoringCredentialIfUserIsEmpty(t *testing.T) {
	tokenSpecs := []*secureTokenSpec{
		newMfaTokenSpec("mfaHost.com", ""),
		newIDTokenSpec("idHost.com", ""),
	}

	for _, tokenSpec := range tokenSpecs {
		t.Run(tokenSpec.input.snowflake, func(t *testing.T) {
			credentialsStorage.setCredential(tokenSpec, "non-empty-value")
			assertEqualE(t, credentialsStorage.getCredential(tokenSpec), "")
		})
	}
}

func TestSkipStoringCredentialIfHostIsEmpty(t *testing.T) {
	tokenSpecs := []*secureTokenSpec{
		newMfaTokenSpec("", "mfaUser"),
		newIDTokenSpec("", "idUser"),
	}

	for _, tokenSpec := range tokenSpecs {
		t.Run(tokenSpec.input.username, func(t *testing.T) {
			credentialsStorage.setCredential(tokenSpec, "non-empty-value")
			assertEqualE(t, credentialsStorage.getCredential(tokenSpec), "")
		})
	}
}

func TestStoreTemporaryCredential(t *testing.T) {
	if runningOnGithubAction() {
		t.Skip("cannot write to github file system")
	}

	testcases := []struct {
		tokenSpec *secureTokenSpec
		value     string
	}{
		{newMfaTokenSpec("testhost", "testuser"), "mfa token"},
		{newIDTokenSpec("testhost", "testuser"), "id token"},
		{newOAuthAccessTokenSpec("https://idp.example.com/token", "testhost", "testuser", "testrole"), "access token"},
		{newOAuthRefreshTokenSpec("https://idp.example.com/token", "testhost", "testuser", "testrole"), "refresh token"},
	}

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
	// Golden vector A: OAuth flow with DPOP_BUNDLED_ACCESS_TOKEN.
	// Raw inputs go through normalizeURL and normalizeIdentifier;
	// lowercase inside quotes is preserved verbatim.
	// Expected canonical JSON (4 OAuth fields, no token_type):
	//   {"idp":"LOGIN.MICROSOFTONLINE.COM:443/TENANT-ID/OAUTH2/V2.0",
	//    "role":"\"Analyst Role With Spaces\":NORTH_AMERICA:PROD:READONLY",
	//    "snowflake":"MYORG-MYACCOUNT.PRIVATELINK.SNOWFLAKECOMPUTING.COM",
	//    "username":"\"First Last\"@LONG-CORPORATE-DOMAIN.EXAMPLE.COM"}
	key, err := buildCacheKey(cacheKeyInput{
		tokenType: "DPOP_BUNDLED_ACCESS_TOKEN",
		idp:       "https://login.microsoftonline.com:443/tenant-id/oauth2/v2.0",
		snowflake: "https://myorg-myaccount.privatelink.snowflakecomputing.com",
		username:  `"First Last"@long-corporate-domain.example.com`,
		role:      `"Analyst Role With Spaces":north_america:prod:readonly`,
	})
	assertNilF(t, err)
	assertEqualF(t, key, "SnowflakeTokenCache.v2.DPOP_BUNDLED_ACCESS_TOKEN.be782aa7c9abf8698adc9e6de61b954ccec7d9202899b44c2eb4e1dfa4313d5f") // pragma: allowlist secret
}

func TestBuildCacheKeyMfaGoldenHash(t *testing.T) {
	// Golden vector B: MFA flow — only snowflake + username in keyData.
	// Expected canonical JSON (2 MFA fields, no idp/role/token_type):
	//   {"snowflake":"MYORG-MYACCOUNT.PRIVATELINK.SNOWFLAKECOMPUTING.COM",
	//    "username":"\"First Last\"@LONG-CORPORATE-DOMAIN.EXAMPLE.COM"}
	key, err := buildCacheKey(cacheKeyInput{
		tokenType: mfaToken,
		snowflake: "https://myorg-myaccount.privatelink.snowflakecomputing.com",
		username:  `"First Last"@long-corporate-domain.example.com`,
	})
	assertNilF(t, err)
	assertEqualF(t, key, "SnowflakeTokenCache.v2.MFA_TOKEN.a508fa2858a6e22e9fdbc90b4149a3ff666d1acbb286c85ff179499ac92d75c8") // pragma: allowlist secret
}

func TestBuildCacheKeyValidation(t *testing.T) {
	t.Run("empty snowflake rejects", func(t *testing.T) {
		_, err := buildCacheKey(cacheKeyInput{
			tokenType: mfaToken,
			snowflake: "",
			username:  "user",
		})
		assertNotNilF(t, err)
		assertStringContainsE(t, err.Error(), "snowflake URL is required")
	})

	t.Run("empty username rejects", func(t *testing.T) {
		_, err := buildCacheKey(cacheKeyInput{
			tokenType: mfaToken,
			snowflake: "host.com",
			username:  "",
		})
		assertNotNilF(t, err)
		assertStringContainsE(t, err.Error(), "username is required")
	})
}

func TestNormalizeURL(t *testing.T) {
	testcases := []struct {
		input    string
		expected string
	}{
		{"https://login.microsoftonline.com:443/tenant-id/oauth2/v2.0", "LOGIN.MICROSOFTONLINE.COM:443/TENANT-ID/OAUTH2/V2.0"},
		{"https://myorg-myaccount.privatelink.snowflakecomputing.com", "MYORG-MYACCOUNT.PRIVATELINK.SNOWFLAKECOMPUTING.COM"},
		{"http://example.com/", "EXAMPLE.COM"},
		{"https://user:pass@host.com/path", "HOST.COM/PATH"},
		{"host.com", "HOST.COM"},
		{"https://host.com?query=1#frag", "HOST.COM"},
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
		{`"First Last"@long-corporate-domain.example.com`, `"First Last"@LONG-CORPORATE-DOMAIN.EXAMPLE.COM`},
		{`"Analyst Role With Spaces":north_america:prod:readonly`, `"Analyst Role With Spaces":NORTH_AMERICA:PROD:READONLY`},
		{"simpleRole", "SIMPLEROLE"},
		{`"CaseSensitive"`, `"CaseSensitive"`},
		{"", ""},
	}
	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			assertEqualE(t, normalizeIdentifier(tc.input), tc.expected)
		})
	}
}

func TestDifferentSnowflakeHostsProduceDifferentKeys(t *testing.T) {
	key1, err := buildCacheKey(cacheKeyInput{
		tokenType: oauthAccessToken,
		idp:       "https://idp.example.com/token",
		snowflake: "https://account1.snowflakecomputing.com",
		username:  "user",
		role:      "ANALYST",
	})
	assertNilF(t, err)

	key2, err := buildCacheKey(cacheKeyInput{
		tokenType: oauthAccessToken,
		idp:       "https://idp.example.com/token",
		snowflake: "https://account2.snowflakecomputing.com",
		username:  "user",
		role:      "ANALYST",
	})
	assertNilF(t, err)

	assertNotEqualF(t, key1, key2)
}

func TestDifferentRolesProduceDifferentKeys(t *testing.T) {
	key1, err := buildCacheKey(cacheKeyInput{
		tokenType: oauthAccessToken,
		idp:       "https://idp.example.com/token",
		snowflake: "https://account.snowflakecomputing.com",
		username:  "user",
		role:      "ANALYST",
	})
	assertNilF(t, err)

	key2, err := buildCacheKey(cacheKeyInput{
		tokenType: oauthAccessToken,
		idp:       "https://idp.example.com/token",
		snowflake: "https://account.snowflakecomputing.com",
		username:  "user",
		role:      "ADMIN",
	})
	assertNilF(t, err)

	assertNotEqualF(t, key1, key2)
}

func TestMfaWithEmptyRoleProducesStableKey(t *testing.T) {
	key1, err := buildCacheKey(cacheKeyInput{
		tokenType: mfaToken,
		snowflake: "https://account.snowflakecomputing.com",
		username:  "user",
	})
	assertNilF(t, err)

	key2, err := buildCacheKey(cacheKeyInput{
		tokenType: mfaToken,
		snowflake: "https://account.snowflakecomputing.com",
		username:  "user",
	})
	assertNilF(t, err)

	assertEqualF(t, key1, key2)
	assertStringContainsE(t, key1, "SnowflakeTokenCache.v2.MFA_TOKEN.")
}

func TestDifferentTokenTypesProduceDifferentKeys(t *testing.T) {
	t.Run("OAuth access vs refresh", func(t *testing.T) {
		key1, err := buildCacheKey(cacheKeyInput{
			tokenType: oauthAccessToken,
			idp:       "https://idp.example.com/token",
			snowflake: "https://account.snowflakecomputing.com",
			username:  "user",
			role:      "ANALYST",
		})
		assertNilF(t, err)

		key2, err := buildCacheKey(cacheKeyInput{
			tokenType: oauthRefreshToken,
			idp:       "https://idp.example.com/token",
			snowflake: "https://account.snowflakecomputing.com",
			username:  "user",
			role:      "ANALYST",
		})
		assertNilF(t, err)
		assertNotEqualF(t, key1, key2)
	})

	t.Run("MFA vs OAuth for same user/host", func(t *testing.T) {
		mfaKey, err := buildCacheKey(cacheKeyInput{
			tokenType: mfaToken,
			snowflake: "https://account.snowflakecomputing.com",
			username:  "user",
		})
		assertNilF(t, err)

		oauthKey, err := buildCacheKey(cacheKeyInput{
			tokenType: oauthAccessToken,
			idp:       "https://account.snowflakecomputing.com",
			snowflake: "https://account.snowflakecomputing.com",
			username:  "user",
			role:      "",
		})
		assertNilF(t, err)
		assertNotEqualF(t, mfaKey, oauthKey)
	})
}

func TestCacheKeyUsesV2Prefix(t *testing.T) {
	key, err := buildCacheKey(cacheKeyInput{
		tokenType: mfaToken,
		snowflake: "host.com",
		username:  "user",
	})
	assertNilF(t, err)
	assertStringContainsE(t, key, "SnowflakeTokenCache.v2.MFA_TOKEN.")
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

	tokenSpec := newMfaTokenSpec("host.com", "testuser")
	ssm.setCredential(tokenSpec, "testvalue")

	fileContent, err := os.ReadFile(ssm.credFilePath())
	assertNilF(t, err)
	var m map[string]any
	err = json.Unmarshal(fileContent, &m)
	assertNilF(t, err)

	expectedKey, err := tokenSpec.buildKey()
	assertNilF(t, err)
	assertStringContainsE(t, expectedKey, "SnowflakeTokenCache.v2.MFA_TOKEN.")

	tokens := m["tokens"].(map[string]any)
	assertEqualE(t, tokens[expectedKey], "testvalue")
}

func TestMfaKeyHasNoIdpOrRole(t *testing.T) {
	// MFA keyData must contain only snowflake + username — no idp or role.
	// Verify by computing the hash of the expected 2-field JSON manually.
	expected2FieldJSON := `{"snowflake":"HOST.COM","username":"USER"}`
	sum := sha256.Sum256([]byte(expected2FieldJSON))
	expectedHash := hex.EncodeToString(sum[:])

	key, err := buildCacheKey(cacheKeyInput{
		tokenType: mfaToken,
		snowflake: "host.com",
		username:  "user",
	})
	assertNilF(t, err)
	assertEqualF(t, key, "SnowflakeTokenCache.v2.MFA_TOKEN."+expectedHash)
}

func TestOAuthKeyHasFourFields(t *testing.T) {
	// OAuth keyData must contain idp, role, snowflake, username — 4 fields.
	expected4FieldJSON := `{"idp":"IDP.COM","role":"ANALYST","snowflake":"HOST.COM","username":"USER"}`
	sum := sha256.Sum256([]byte(expected4FieldJSON))
	expectedHash := hex.EncodeToString(sum[:])

	key, err := buildCacheKey(cacheKeyInput{
		tokenType: oauthAccessToken,
		idp:       "idp.com",
		snowflake: "host.com",
		username:  "user",
		role:      "analyst",
	})
	assertNilF(t, err)
	assertEqualF(t, key, "SnowflakeTokenCache.v2.OAUTH_ACCESS_TOKEN."+expectedHash)
}
