// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"os"
	"testing"
)

type EnvOverride struct {
	env      string
	oldValue string
}

func (e *EnvOverride) rollback() {
	if e.oldValue != "" {
		os.Setenv(e.env, e.oldValue)
	} else {
		os.Unsetenv(e.env)
	}
}

func override_env(env string, value string) EnvOverride {
	oldValue := os.Getenv(env)
	os.Setenv(env, value)
	return EnvOverride{env, oldValue}
}

func TestSnowflakeFileBasedSecureStorageManager(t *testing.T) {
	//skipOnNonLinux(t, "Not supported on non-linux")
	os.Mkdir("./testdata", 0777)
	credCacheDirEnvOverride := override_env(credCacheDirEnv, "./testdata")
	defer credCacheDirEnvOverride.rollback()
	fbss, err := newFileBasedSecureStorageManager()
	if err != nil {
		t.Fatal(err)
	}

	tokenSpec := newMfaTokenSpec("host.xd", "johndoe")
	cred := "token123"
	fbss.setCredential(tokenSpec, cred)
	assertEqualE(t, fbss.getCredential(tokenSpec), cred)
	fbss.deleteCredential(tokenSpec)
	assertEqualE(t, fbss.getCredential(tokenSpec), "")
}

func TestSetAndGetCredentialMfa(t *testing.T) {
	for _, tokenSpec := range []*secureTokenSpec{
		newMfaTokenSpec("testhost", "testuser"),
		newIDTokenSpec("testhost", "testuser"),
	} {
		t.Run(string(tokenSpec.tokenType), func(t *testing.T) {
			skipOnMac(t, "keyring asks for password")
			fakeMfaToken := "test token"
			tokenSpec := newMfaTokenSpec("testHost", "testUser")
			credentialsStorage.setCredential(tokenSpec, fakeMfaToken)
			assertEqualE(t, credentialsStorage.getCredential(tokenSpec), fakeMfaToken)

			// delete credential and check it no longer exists
			credentialsStorage.deleteCredential(tokenSpec)
			assertEqualE(t, credentialsStorage.getCredential(tokenSpec), "")
		})
	}
}

func TestStoreTemporaryCredental(t *testing.T) {
	if runningOnGithubAction() {
		t.Skip("cannot write to github file system")
	}

	testcases := []struct {
		tokenSpec *secureTokenSpec
		value     string
	}{
		{newMfaTokenSpec("testhost", "testuser"), "598ghFnjfh8BBgmf45mmhgkfRR45mgkt5"},
		{newIDTokenSpec("testhost", "testuser"), "090Arftf54Jk3gh57ggrVvf09lJa3DD"},
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

func TestBuildCredentialsKey(t *testing.T) {
	testcases := []struct {
		host     string
		user     string
		credType tokenType
		out      string
	}{
		{"testaccount.snowflakecomputing.com", "testuser", "mfaToken", "TESTACCOUNT.SNOWFLAKECOMPUTING.COM:TESTUSER:SNOWFLAKE-GO-DRIVER:MFATOKEN"},
		{"testaccount.snowflakecomputing.com", "testuser", "IdToken", "TESTACCOUNT.SNOWFLAKECOMPUTING.COM:TESTUSER:SNOWFLAKE-GO-DRIVER:IDTOKEN"},
	}
	for _, test := range testcases {
		target := buildCredentialsKey(test.host, test.user, test.credType)
		if target != test.out {
			t.Fatalf("failed to convert target. expected: %v, but got: %v", test.out, target)
		}
	}
}
