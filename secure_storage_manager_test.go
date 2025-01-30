// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"testing"
)

type tcTargets struct {
	host     string
	user     string
	credType string
	out      string
}

type tcCredentials struct {
	credType string
	token    string
}

func TestSetAndGetCredentialMfa(t *testing.T) {
	skipOnMac(t, "keyring asks for password")
	fakeMfaToken := "fakeMfaToken"
	expectedMfaToken := "fakeMfaToken"
	sc := getDefaultSnowflakeConn()
	sc.cfg.Host = "testhost"
	credentialsStorage.setCredential(sc, mfaToken, fakeMfaToken)
	credentialsStorage.getCredential(sc, mfaToken)

	if sc.cfg.MfaToken != expectedMfaToken {
		t.Fatalf("Expected mfa token %v but got %v", expectedMfaToken, sc.cfg.MfaToken)
	}

	// delete credential and check it no longer exists
	credentialsStorage.deleteCredential(sc, mfaToken)
	credentialsStorage.getCredential(sc, mfaToken)
	if sc.cfg.MfaToken != "" {
		t.Fatalf("Expected mfa token to be empty but got %v", sc.cfg.MfaToken)
	}
}

func TestSetAndGetCredentialIdToken(t *testing.T) {
	skipOnMac(t, "keyring asks for password")
	fakeIDToken := "fakeIDToken"
	expectedIDToken := "fakeIDToken"
	sc := getDefaultSnowflakeConn()
	sc.cfg.Host = "testhost"
	credentialsStorage.setCredential(sc, idToken, fakeIDToken)
	credentialsStorage.getCredential(sc, idToken)

	if sc.cfg.IDToken != expectedIDToken {
		t.Fatalf("Expected id token %v but got %v", expectedIDToken, sc.cfg.IDToken)
	}

	// delete credential and check it no longer exists
	credentialsStorage.deleteCredential(sc, idToken)
	credentialsStorage.getCredential(sc, idToken)
	if sc.cfg.IDToken != "" {
		t.Fatalf("Expected id token to be empty but got %v", sc.cfg.IDToken)
	}
}

func TestStoreTemporaryCredental(t *testing.T) {
	if runningOnGithubAction() {
		t.Skip("cannot write to github file system")
	}

	testcases := []tcCredentials{
		{mfaToken, "598ghFnjfh8BBgmf45mmhgkfRR45mgkt5"},
		{idToken, "090Arftf54Jk3gh57ggrVvf09lJa3DD"},
	}

	ssm := newFileBasedSecureStorageManager()
	_, ok := ssm.(*fileBasedSecureStorageManager)
	assertTrueF(t, ok)

	sc := getDefaultSnowflakeConn()
	for _, test := range testcases {
		t.Run(test.token, func(t *testing.T) {
			ssm.setCredential(sc, test.credType, test.token)
			ssm.getCredential(sc, test.credType)
			if test.credType == mfaToken {
				assertEqualE(t, sc.cfg.MfaToken, test.token)
			} else {
				assertEqualE(t, sc.cfg.IDToken, test.token)
			}
			ssm.deleteCredential(sc, test.credType)
			ssm.getCredential(sc, test.credType)
			if test.credType == mfaToken {
				assertEqualE(t, sc.cfg.MfaToken, "")
			} else {
				assertEqualE(t, sc.cfg.IDToken, "")
			}
		})
	}
}

func TestBuildCredentialsKey(t *testing.T) {
	testcases := []tcTargets{
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
