package gosnowflake

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
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
		err := ssm.setCredential(tokenSpec, "token123")
		assertNilF(t, err)
		cred, err := ssm.getCredential(tokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "token123")
		err = ssm.deleteCredential(tokenSpec)
		assertNilF(t, err)
		cred, err = ssm.getCredential(tokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "")
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
		err := ssm.setCredential(mfaTokenSpec, mfaCred)
		assertNilF(t, err)
		err = ssm.setCredential(idTokenSpec, idCred)
		assertNilF(t, err)
		err = ssm.setCredential(idTokenSpec2, idCred2)
		assertNilF(t, err)
		err = ssm.setCredential(idTokenSpec3, idCred3)
		assertNilF(t, err)
		cred, err := ssm.getCredential(mfaTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, mfaCred)
		cred, err = ssm.getCredential(idTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, idCred)
		cred, err = ssm.getCredential(idTokenSpec2)
		assertNilF(t, err)
		assertEqualE(t, cred, idCred2)
		cred, err = ssm.getCredential(idTokenSpec3)
		assertNilF(t, err)
		assertEqualE(t, cred, idCred3)
		err = ssm.deleteCredential(mfaTokenSpec)
		assertNilF(t, err)
		cred, err = ssm.getCredential(mfaTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "")
		cred, err = ssm.getCredential(idTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, idCred)
		cred, err = ssm.getCredential(idTokenSpec2)
		assertNilF(t, err)
		assertEqualE(t, cred, idCred2)
		cred, err = ssm.getCredential(idTokenSpec3)
		assertNilF(t, err)
		assertEqualE(t, cred, idCred3)
	})

	t.Run("override single token", func(t *testing.T) {
		mfaTokenSpec := newMfaTokenSpec("host.com", "johndoe")
		mfaCred := "token123"
		idTokenSpec := newIDTokenSpec("host.com", "johndoe")
		idCred := "token456"
		err := ssm.setCredential(mfaTokenSpec, mfaCred)
		assertNilF(t, err)
		err = ssm.setCredential(idTokenSpec, idCred)
		assertNilF(t, err)
		cred, err := ssm.getCredential(mfaTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, mfaCred)
		mfaCredOverride := "token789"
		err = ssm.setCredential(mfaTokenSpec, mfaCredOverride)
		assertNilF(t, err)
		cred, err = ssm.getCredential(mfaTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, mfaCredOverride)
		err = ssm.setCredential(idTokenSpec, idCred)
		assertNilF(t, err)
	})

	t.Run("unlock stale cache", func(t *testing.T) {
		tokenSpec := newMfaTokenSpec("stale", "cache")
		assertNilF(t, os.Mkdir(ssm.lockPath(), 0700))
		time.Sleep(1000 * time.Millisecond)
		err := ssm.setCredential(tokenSpec, "unlocked")
		assertNilF(t, err)
		cred, err := ssm.getCredential(tokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "unlocked")
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
		err := ssm.setCredential(tokenSpec, "unlocked")
		assertNilF(t, err)
		totalDurationMillis := time.Since(startTime).Milliseconds()
		cred, err := ssm.getCredential(tokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "unlocked")
		assertTrueE(t, totalDurationMillis > 1000 && totalDurationMillis < 1200)
	})

	t.Run("should not modify keys other than tokens", func(t *testing.T) {
		content := []byte(`{
			"otherKey": "otherValue"
		}`)
		err := os.WriteFile(ssm.credFilePath(), content, 0600)
		assertNilF(t, err)
		err = ssm.setCredential(newMfaTokenSpec("somehost.com", "someUser"), "someToken")
		assertNilF(t, err)
		result, err := os.ReadFile(ssm.credFilePath())
		assertNilF(t, err)
		assertStringContainsE(t, string(result), `"otherKey":"otherValue"`)
	})

	t.Run("should not modify file if it has wrong permission", func(t *testing.T) {
		tokenSpec := newMfaTokenSpec("somehost.com", "someUser")
		err = ssm.setCredential(tokenSpec, "initialValue")
		assertNilF(t, err)
		cred, err := ssm.getCredential(tokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "initialValue")
		err = os.Chmod(ssm.credFilePath(), 0644)
		assertNilF(t, err)
		defer func() {
			assertNilE(t, os.Chmod(ssm.credFilePath(), 0600))
		}()
		err = ssm.setCredential(tokenSpec, "newValue")
		assertNilF(t, err)
		cred, err = ssm.getCredential(tokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "")
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
		err = ssm.setCredential(tokenSpec, "initialValue")
		assertNilF(t, err)
		cred, err := ssm.getCredential(tokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "initialValue")
		err = os.Chmod(ssm.credDirPath, 0777)
		assertNilF(t, err)
		defer func() {
			assertNilE(t, os.Chmod(ssm.credDirPath, 0700))
		}()
		err = ssm.setCredential(tokenSpec, "newValue")
		assertNilF(t, err)
		cred, err = ssm.getCredential(tokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "")
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

func TestSetAndGetCredential(t *testing.T) {
	for _, tokenSpec := range []*secureTokenSpec{
		newMfaTokenSpec("testhost", "testuser"),
		newIDTokenSpec("testhost", "testuser"),
	} {
		t.Run(string(tokenSpec.tokenType), func(t *testing.T) {
			skipOnMac(t, "keyring asks for password")
			fakeMfaToken := "test token"
			tokenSpec := newMfaTokenSpec("testHost", "testUser")
			err := credentialsStorage.setCredential(tokenSpec, fakeMfaToken)
			assertNilF(t, err)
			cred, err := credentialsStorage.getCredential(tokenSpec)
			assertNilF(t, err)
			assertEqualE(t, cred, fakeMfaToken)

			// delete credential and check it no longer exists
			err = credentialsStorage.deleteCredential(tokenSpec)
			assertNilF(t, err)
			cred, err = credentialsStorage.getCredential(tokenSpec)
			assertNilF(t, err)
			assertEqualE(t, cred, "")
		})
	}
}

func TestSkipStoringCredentialIfUserIsEmpty(t *testing.T) {
	tokenSpecs := []*secureTokenSpec{
		newMfaTokenSpec("mfaHost.com", ""),
		newIDTokenSpec("idHost.com", ""),
	}

	for _, tokenSpec := range tokenSpecs {
		t.Run(tokenSpec.host, func(t *testing.T) {
			err := credentialsStorage.setCredential(tokenSpec, "non-empty-value")
			assertNilF(t, err)
			cred, err := credentialsStorage.getCredential(tokenSpec)
			assertNilF(t, err)
			assertEqualE(t, cred, "")
		})
	}
}

func TestSkipStoringCredentialIfHostIsEmpty(t *testing.T) {
	tokenSpecs := []*secureTokenSpec{
		newMfaTokenSpec("", "mfaUser"),
		newIDTokenSpec("", "idUser"),
	}

	for _, tokenSpec := range tokenSpecs {
		t.Run(tokenSpec.user, func(t *testing.T) {
			err := credentialsStorage.setCredential(tokenSpec, "non-empty-value")
			assertNilF(t, err)
			cred, err := credentialsStorage.getCredential(tokenSpec)
			assertNilF(t, err)
			assertEqualE(t, cred, "")
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
		{newOAuthAccessTokenSpec("testhost", "testuser"), "access token"},
		{newOAuthRefreshTokenSpec("testhost", "testuser"), "refresh token"},
	}

	ssm, err := newFileBasedSecureStorageManager()
	assertNilF(t, err)

	for _, test := range testcases {
		t.Run(test.value, func(t *testing.T) {
			err := ssm.setCredential(test.tokenSpec, test.value)
			assertNilF(t, err)
			cred, err := ssm.getCredential(test.tokenSpec)
			assertNilF(t, err)
			assertEqualE(t, cred, test.value)
			err = ssm.deleteCredential(test.tokenSpec)
			assertNilF(t, err)
			cred, err = ssm.getCredential(test.tokenSpec)
			assertNilF(t, err)
			assertEqualE(t, cred, "")
		})
	}
}

func TestBuildCredentialsKey(t *testing.T) {
	testcases := []struct {
		host     string
		user     string
		credType tokenType
		out      string
	}{
		{"testaccount.snowflakecomputing.com", "testuser", "mfaToken", "c4e781475e7a5e74aca87cd462afafa8cc48ebff6f6ccb5054b894dae5eb6345"}, // pragma: allowlist secret
		{"testaccount.snowflakecomputing.com", "testuser", "IdToken", "5014e26489992b6ea56b50e936ba85764dc51338f60441bdd4a69eac7e15bada"},  // pragma: allowlist secret
	}
	for _, test := range testcases {
		target, err := buildCredentialsKey(test.host, test.user, test.credType)
		assertNilF(t, err)
		if target != test.out {
			t.Fatalf("failed to convert target. expected: %v, but got: %v", test.out, target)
		}
	}
}
