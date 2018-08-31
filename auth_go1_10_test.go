// Copyright (c) 2017-2018 Snowflake Computing Inc. All right reserved.
// +build go1.10

package gosnowflake

// This file contains authentication tests that should only be built and ran under
// Golang version 1.10 or upper

import (
	"crypto/rand"
	"crypto/rsa"
	"testing"
)

// Test JWT function in the local environment against the validation function in go
func TestUnitAuthenticateJWT(t *testing.T) {
	var err error

	sr := &snowflakeRestful{
		FuncPostAuth: postAuthCheckJWTToken,
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Authenticator = authenticatorJWT
	sc.cfg.JWTExpireTimeout = defaultJWTTimeout
	sc.cfg.PrivateKey = testPrivKey
	sc.rest = sr

	// A valid JWT token should pass
	_, err = authenticate(sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}

	// An invalid JWT token should not pass
	invalidPrivateKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	sc.cfg.PrivateKey = invalidPrivateKey
	_, err = authenticate(sc, []byte{}, []byte{})
	if err == nil {
		t.Fatalf("invalid token passed")
	}

}
