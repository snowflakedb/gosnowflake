// Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestGetTokenFromResponseFail(t *testing.T) {
	response := "GET /?fakeToken=fakeEncodedSamlToken HTTP/1.1\r\n" +
		"Host: localhost:54001\r\n" +
		"Connection: keep-alive\r\n" +
		"Upgrade-Insecure-Requests: 1\r\n" +
		"User-Agent: userAgentStr\r\n" +
		"Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8\r\n" +
		"Referer: https://myaccount.snowflakecomputing.com/fed/login\r\n" +
		"Accept-Encoding: gzip, deflate, br\r\n" +
		"Accept-Language: en-US,en;q=0.9\r\n\r\n"

	_, err := getTokenFromResponse(response)
	if err == nil {
		t.Errorf("Should have failed parsing the malformed response.")
	}
}

func TestGetTokenFromResponse(t *testing.T) {
	response := "GET /?token=GETtokenFromResponse HTTP/1.1\r\n" +
		"Host: localhost:54001\r\n" +
		"Connection: keep-alive\r\n" +
		"Upgrade-Insecure-Requests: 1\r\n" +
		"User-Agent: userAgentStr\r\n" +
		"Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8\r\n" +
		"Referer: https://myaccount.snowflakecomputing.com/fed/login\r\n" +
		"Accept-Encoding: gzip, deflate, br\r\n" +
		"Accept-Language: en-US,en;q=0.9\r\n\r\n"

	expected := "GETtokenFromResponse"

	token, err := getTokenFromResponse(response)
	if err != nil {
		t.Errorf("Failed to get the token. Err: %#v", err)
	}
	if token != expected {
		t.Errorf("Expected: %s, found: %s", expected, token)
	}
}

func TestBuildResponse(t *testing.T) {
	resp := buildResponse("Go")
	bytes := resp.Bytes()
	respStr := string(bytes[:])
	if !strings.Contains(respStr, "Your identity was confirmed and propagated to Snowflake Go.\nYou can close this window now and go back where you started from.") {
		t.Fatalf("failed to build response")
	}
}

func postAuthExternalBrowserError(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{}, errors.New("failed to get SAML response")
}

func postAuthExternalBrowserFail(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: false,
		Message: "external browser auth failed",
	}, nil
}

func postAuthExternalBrowserFailWithCode(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: false,
		Message: "failed to connect to db",
		Code:    "260008",
	}, nil
}

func TestUnitAuthenticateByExternalBrowser(t *testing.T) {
	authenticator := "externalbrowser"
	application := "testapp"
	account := "testaccount"
	user := "u"
	password := "p"
	sr := &snowflakeRestful{
		Protocol:         "https",
		Host:             "abc.com",
		Port:             443,
		FuncPostAuthSAML: postAuthExternalBrowserError,
		TokenAccessor:    getSimpleTokenAccessor(),
	}
	_, _, err := authenticateByExternalBrowser(context.TODO(), sr, authenticator, application, account, user, password)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPostAuthSAML = postAuthExternalBrowserFail
	_, _, err = authenticateByExternalBrowser(context.TODO(), sr, authenticator, application, account, user, password)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPostAuthSAML = postAuthExternalBrowserFailWithCode
	_, _, err = authenticateByExternalBrowser(context.TODO(), sr, authenticator, application, account, user, password)
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("should be snowflake error. err: %v", err)
	}
	if driverErr.Number != ErrCodeFailedToConnect {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrCodeFailedToConnect, driverErr.Number)
	}
}
