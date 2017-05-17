// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"net/url"
	"testing"
	"time"
)

func TestUnitPostAuth(t *testing.T) {
	sr := &snowflakeRestful{
		Token:    "token",
		FuncPost: postTestAfterRenew,
	}
	var err error
	_, err = postAuth(sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sr.FuncPost = postTestError
	_, err = postAuth(sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0)
	if err == nil {
		t.Fatal("should have failed to auth for unknown reason")
	}
	sr.FuncPost = postTestAppBadGatewayError
	_, err = postAuth(sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0)
	if err == nil {
		t.Fatal("should have failed to auth for unknown reason")
	}
	sr.FuncPost = postTestAppForbiddenError
	_, err = postAuth(sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0)
	if err == nil {
		t.Fatal("should have failed to auth for unknown reason")
	}
	sr.FuncPost = postTestAppUnexpectedError
	_, err = postAuth(sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0)
	if err == nil {
		t.Fatal("should have failed to auth for unknown reason")
	}
}

func postAuthFailServiceIssue(_ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return nil, &SnowflakeError{
		Number: ErrServiceUnavailable,
	}
}

func postAuthFailWrongAccount(_ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return nil, &SnowflakeError{
		Number: ErrFailedToConnect,
	}
}

func postAuthFailUnknown(_ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return nil, &SnowflakeError{
		Number: ErrFailedToAuth,
	}
}

func postAuthSuccessWithErrorCode(_ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: false,
		Code:    "98765",
		Message: "wrong!",
	}, nil
}

func postAuthSuccess(_ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: true,
		Data: authResponseMain{
			Token:       "t",
			MasterToken: "m",
			SessionInfo: authResponseSessionInfo{
				DatabaseName: "dbn",
			},
		},
	}, nil
}

func TestUnitAuthenticate(t *testing.T) {
	var err error
	var driverErr *SnowflakeError
	var ok bool
	sr := &snowflakeRestful{
		FuncPostAuth: postAuthFailServiceIssue,
	}
	_, err = authenticate(
		sr, "u", "p", "a", "d",
		"s", "w", "r", "", false,
		"testapp", []byte{}, "", "")
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrServiceUnavailable {
		t.Fatalf("Snowflake error is expected. err: %v", driverErr)
	}
	sr.FuncPostAuth = postAuthFailWrongAccount
	_, err = authenticate(
		sr, "u", "p", "a", "d",
		"s", "w", "r", "", false,
		"testapp", []byte{}, "", "")
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrFailedToConnect {
		t.Fatalf("Snowflake error is expected. err: %v", driverErr)
	}
	sr.FuncPostAuth = postAuthFailUnknown
	_, err = authenticate(
		sr, "u", "p", "a", "d",
		"s", "w", "r", "", false,
		"testapp", []byte{}, "", "")
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrFailedToAuth {
		t.Fatalf("Snowflake error is expected. err: %v", driverErr)
	}
	sr.FuncPostAuth = postAuthSuccessWithErrorCode
	_, err = authenticate(
		sr, "u", "p", "a", "d",
		"s", "w", "r", "", false,
		"testapp", []byte{}, "", "")
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != 98765 {
		t.Fatalf("Snowflake error is expected. err: %v", driverErr)
	}
	sr.FuncPostAuth = postAuthSuccess
	var resp *authResponseSessionInfo
	resp, err = authenticate(
		sr, "u", "p", "a", "d",
		"s", "w", "r", "", false,
		"testapp", []byte{}, "", "")
	if err != nil {
		t.Fatalf("failed to auth. err: %v", err)
	}
	if resp.DatabaseName != "dbn" {
		t.Fatalf("failed to get response from auth")
	}
}
