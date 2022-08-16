// Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"testing"
)

func TestSetAndGetCredentialMfa(t *testing.T) {
	fakeMfaToken := "fakeMfaToken"
	expectedMfaToken := "fakeMfaToken"
	sc := getDefaultSnowflakeConn()
	sc.cfg.Host = "testhost"
	setCredential(sc, mfaToken, fakeMfaToken)
	getCredential(sc, mfaToken)

	if sc.cfg.MfaToken != expectedMfaToken {
		t.Fatalf("Expected mfa token %v but got %v", expectedMfaToken, sc.cfg.MfaToken)
	}

	// delete credential and check it no longer exists
	deleteCredential(sc, mfaToken)
	getCredential(sc, mfaToken)
	if sc.cfg.MfaToken != "" {
		t.Fatalf("Expected mfa token to be empty but got %v", sc.cfg.MfaToken)
	}
}

func TestSetAndGetCredentialIdToken(t *testing.T) {
	fakeIDToken := "fakeIDToken"
	expectedIDToken := "fakeIDToken"
	sc := getDefaultSnowflakeConn()
	sc.cfg.Host = "testhost"
	setCredential(sc, idToken, fakeIDToken)
	getCredential(sc, idToken)

	if sc.cfg.IDToken != expectedIDToken {
		t.Fatalf("Expected id token %v but got %v", expectedIDToken, sc.cfg.IDToken)
	}

	// delete credential and check it no longer exists
	deleteCredential(sc, idToken)
	getCredential(sc, idToken)
	if sc.cfg.IDToken != "" {
		t.Fatalf("Expected id token to be empty but got %v", sc.cfg.IDToken)
	}
}
