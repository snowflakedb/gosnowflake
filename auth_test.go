// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestUnitPostAuth(t *testing.T) {
	sr := &snowflakeRestful{
		TokenAccessor: getSimpleTokenAccessor(),
		FuncAuthPost:  postAuthTestAfterRenew,
	}
	var err error
	bodyCreator := func() ([]byte, error) {
		return []byte{0x12, 0x34}, nil
	}
	_, err = postAuth(context.Background(), sr, sr.Client, &url.Values{}, make(map[string]string), bodyCreator, 0)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sr.FuncAuthPost = postAuthTestError
	_, err = postAuth(context.Background(), sr, sr.Client, &url.Values{}, make(map[string]string), bodyCreator, 0)
	if err == nil {
		t.Fatal("should have failed to auth for unknown reason")
	}
	sr.FuncAuthPost = postAuthTestAppBadGatewayError
	_, err = postAuth(context.Background(), sr, sr.Client, &url.Values{}, make(map[string]string), bodyCreator, 0)
	if err == nil {
		t.Fatal("should have failed to auth for unknown reason")
	}
	sr.FuncAuthPost = postAuthTestAppForbiddenError
	_, err = postAuth(context.Background(), sr, sr.Client, &url.Values{}, make(map[string]string), bodyCreator, 0)
	if err == nil {
		t.Fatal("should have failed to auth for unknown reason")
	}
	sr.FuncAuthPost = postAuthTestAppUnexpectedError
	_, err = postAuth(context.Background(), sr, sr.Client, &url.Values{}, make(map[string]string), bodyCreator, 0)
	if err == nil {
		t.Fatal("should have failed to auth for unknown reason")
	}
}

func postAuthFailServiceIssue(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, _ bodyCreatorType, _ time.Duration) (*authResponse, error) {
	return nil, &SnowflakeError{
		Number: ErrCodeServiceUnavailable,
	}
}

func postAuthFailWrongAccount(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, _ bodyCreatorType, _ time.Duration) (*authResponse, error) {
	return nil, &SnowflakeError{
		Number: ErrCodeFailedToConnect,
	}
}

func postAuthFailUnknown(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, _ bodyCreatorType, _ time.Duration) (*authResponse, error) {
	return nil, &SnowflakeError{
		Number: ErrFailedToAuth,
	}
}

func postAuthSuccessWithErrorCode(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, _ bodyCreatorType, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: false,
		Code:    "98765",
		Message: "wrong!",
	}, nil
}

func postAuthSuccessWithInvalidErrorCode(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, _ bodyCreatorType, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: false,
		Code:    "abcdef",
		Message: "wrong!",
	}, nil
}

func postAuthSuccess(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, _ bodyCreatorType, _ time.Duration) (*authResponse, error) {
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

func postAuthCheckSAMLResponse(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, bodyCreator bodyCreatorType, _ time.Duration) (*authResponse, error) {
	var ar authRequest
	jsonBody, err := bodyCreator()
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}
	if ar.Data.RawSAMLResponse == "" {
		return nil, errors.New("SAML response is empty")
	}
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

// Checks that the request body generated when authenticating with OAuth
// contains all the necessary values.
func postAuthCheckOAuth(
	_ context.Context,
	_ *snowflakeRestful,
	_ *http.Client,
	_ *url.Values, _ map[string]string,
	bodyCreator bodyCreatorType,
	_ time.Duration,
) (*authResponse, error) {
	var ar authRequest
	jsonBody, _ := bodyCreator()
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}
	if ar.Data.Authenticator != AuthTypeOAuth.String() {
		return nil, errors.New("Authenticator is not OAUTH")
	}
	if ar.Data.Token == "" {
		return nil, errors.New("Token is empty")
	}
	if ar.Data.LoginName == "" {
		return nil, errors.New("Login name is empty")
	}
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

func postAuthCheckPasscode(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, bodyCreator bodyCreatorType, _ time.Duration) (*authResponse, error) {
	var ar authRequest
	jsonBody, _ := bodyCreator()
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}
	if ar.Data.Passcode != "987654321" || ar.Data.ExtAuthnDuoMethod != "passcode" {
		return nil, fmt.Errorf("passcode didn't match. expected: 987654321, got: %v, duo: %v", ar.Data.Passcode, ar.Data.ExtAuthnDuoMethod)
	}
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

func postAuthCheckPasscodeInPassword(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, bodyCreator bodyCreatorType, _ time.Duration) (*authResponse, error) {
	var ar authRequest
	jsonBody, _ := bodyCreator()
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}
	if ar.Data.Passcode != "" || ar.Data.ExtAuthnDuoMethod != "passcode" {
		return nil, fmt.Errorf("passcode must be empty, got: %v, duo: %v", ar.Data.Passcode, ar.Data.ExtAuthnDuoMethod)
	}
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

// JWT token validate callback function to check the JWT token
// It uses the public key paired with the testPrivKey
func postAuthCheckJWTToken(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, bodyCreator bodyCreatorType, _ time.Duration) (*authResponse, error) {
	var ar authRequest
	jsonBody, _ := bodyCreator()
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}
	if ar.Data.Authenticator != AuthTypeJwt.String() {
		return nil, errors.New("Authenticator is not JWT")
	}

	tokenString := ar.Data.Token

	// Validate token
	_, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Don't forget to validate the alg is what you expect:
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}

		return testPrivKey.Public(), nil
	})
	if err != nil {
		return nil, err
	}

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

func postAuthCheckUsernamePasswordMfa(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, bodyCreator bodyCreatorType, _ time.Duration) (*authResponse, error) {
	var ar authRequest
	jsonBody, _ := bodyCreator()
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}

	if ar.Data.SessionParameters["CLIENT_REQUEST_MFA_TOKEN"] != true {
		return nil, fmt.Errorf("expected client_request_mfa_token to be true but was %v", ar.Data.SessionParameters["CLIENT_REQUEST_MFA_TOKEN"])
	}
	return &authResponse{
		Success: true,
		Data: authResponseMain{
			Token:       "t",
			MasterToken: "m",
			MfaToken:    "mockedMfaToken",
			SessionInfo: authResponseSessionInfo{
				DatabaseName: "dbn",
			},
		},
	}, nil
}

func postAuthCheckUsernamePasswordMfaToken(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, bodyCreator bodyCreatorType, _ time.Duration) (*authResponse, error) {
	var ar authRequest
	jsonBody, _ := bodyCreator()
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}

	if ar.Data.Token != "mockedMfaToken" {
		return nil, fmt.Errorf("unexpected mfa token: %v", ar.Data.Token)
	}
	return &authResponse{
		Success: true,
		Data: authResponseMain{
			Token:       "t",
			MasterToken: "m",
			MfaToken:    "mockedMfaToken",
			SessionInfo: authResponseSessionInfo{
				DatabaseName: "dbn",
			},
		},
	}, nil
}

func postAuthCheckUsernamePasswordMfaFailed(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, bodyCreator bodyCreatorType, _ time.Duration) (*authResponse, error) {
	var ar authRequest
	jsonBody, _ := bodyCreator()
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}

	if ar.Data.Token != "mockedMfaToken" {
		return nil, fmt.Errorf("unexpected mfa token: %v", ar.Data.Token)
	}
	return &authResponse{
		Success: false,
		Data:    authResponseMain{},
		Message: "auth failed",
		Code:    "260008",
	}, nil
}

func postAuthCheckExternalBrowser(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, bodyCreator bodyCreatorType, _ time.Duration) (*authResponse, error) {
	var ar authRequest
	jsonBody, _ := bodyCreator()
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}

	if ar.Data.SessionParameters["CLIENT_STORE_TEMPORARY_CREDENTIAL"] != true {
		return nil, fmt.Errorf("expected client_store_temporary_credential to be true but was %v", ar.Data.SessionParameters["CLIENT_STORE_TEMPORARY_CREDENTIAL"])
	}
	return &authResponse{
		Success: true,
		Data: authResponseMain{
			Token:       "t",
			MasterToken: "m",
			IDToken:     "mockedIDToken",
			SessionInfo: authResponseSessionInfo{
				DatabaseName: "dbn",
			},
		},
	}, nil
}

func postAuthCheckExternalBrowserToken(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, bodyCreator bodyCreatorType, _ time.Duration) (*authResponse, error) {
	var ar authRequest
	jsonBody, _ := bodyCreator()
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}

	if ar.Data.Token != "mockedIDToken" {
		return nil, fmt.Errorf("unexpected mfatoken: %v", ar.Data.Token)
	}
	return &authResponse{
		Success: true,
		Data: authResponseMain{
			Token:       "t",
			MasterToken: "m",
			IDToken:     "mockedIDToken",
			SessionInfo: authResponseSessionInfo{
				DatabaseName: "dbn",
			},
		},
	}, nil
}

func postAuthCheckExternalBrowserFailed(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, bodyCreator bodyCreatorType, _ time.Duration) (*authResponse, error) {
	var ar authRequest
	jsonBody, _ := bodyCreator()
	if err := json.Unmarshal(jsonBody, &ar); err != nil {
		return nil, err
	}

	if ar.Data.SessionParameters["CLIENT_STORE_TEMPORARY_CREDENTIAL"] != true {
		return nil, fmt.Errorf("expected client_store_temporary_credential to be true but was %v", ar.Data.SessionParameters["CLIENT_STORE_TEMPORARY_CREDENTIAL"])
	}
	return &authResponse{
		Success: false,
		Data:    authResponseMain{},
		Message: "auth failed",
		Code:    "260008",
	}, nil
}

func postAuthOktaWithNewToken(_ context.Context, _ *snowflakeRestful, _ *http.Client, _ *url.Values, _ map[string]string, bodyCreator bodyCreatorType, _ time.Duration) (*authResponse, error) {
	var ar authRequest

	cfg := &Config{
		Authenticator: AuthTypeOkta,
	}

	// Retry 3 times and success
	client := &fakeHTTPClient{
		cnt:        3,
		success:    true,
		statusCode: 429,
	}

	urlPtr, err := url.Parse("https://fakeaccountretrylogin.snowflakecomputing.com:443/login-request?request_guid=testguid")
	if err != nil {
		return &authResponse{}, err
	}

	body := func() ([]byte, error) {
		jsonBody, _ := bodyCreator()
		if err := json.Unmarshal(jsonBody, &ar); err != nil {
			return nil, err
		}
		return jsonBody, err
	}

	_, err = newRetryHTTP(context.Background(), client, emptyRequest, urlPtr, make(map[string]string), 60*time.Second, 3, defaultTimeProvider, cfg).doPost().setBodyCreator(body).execute()
	if err != nil {
		return &authResponse{}, err
	}

	return &authResponse{
		Success: true,
		Data: authResponseMain{
			Token:       "t",
			MasterToken: "m",
			MfaToken:    "mockedMfaToken",
			SessionInfo: authResponseSessionInfo{
				DatabaseName: "dbn",
			},
		},
	}, nil
}

func getDefaultSnowflakeConn() *snowflakeConn {
	sc := &snowflakeConn{
		rest: &snowflakeRestful{
			TokenAccessor: getSimpleTokenAccessor(),
		},
		cfg: &Config{
			Account:            "a",
			User:               "u",
			Password:           "p",
			Database:           "d",
			Schema:             "s",
			Warehouse:          "w",
			Role:               "r",
			Region:             "",
			Params:             make(map[string]*string),
			PasscodeInPassword: false,
			Passcode:           "",
			Application:        "testapp",
		},
		telemetry: &snowflakeTelemetry{enabled: false},
	}
	return sc
}

func TestUnitAuthenticateWithTokenAccessor(t *testing.T) {
	expectedSessionID := int64(123)
	expectedMasterToken := "master_token"
	expectedToken := "auth_token"

	ta := getSimpleTokenAccessor()
	ta.SetTokens(expectedToken, expectedMasterToken, expectedSessionID)
	sc := getDefaultSnowflakeConn()
	sc.cfg.Authenticator = AuthTypeTokenAccessor
	sc.cfg.TokenAccessor = ta
	sr := &snowflakeRestful{
		FuncPostAuth:  postAuthFailServiceIssue,
		TokenAccessor: ta,
	}
	sc.rest = sr

	// FuncPostAuth is set to fail, but AuthTypeTokenAccessor should not even make a call to FuncPostAuth
	resp, err := authenticate(context.Background(), sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("should not have failed, err %v", err)
	}

	if resp.SessionID != expectedSessionID {
		t.Fatalf("Expected session id %v but got %v", expectedSessionID, resp.SessionID)
	}
	if resp.Token != expectedToken {
		t.Fatalf("Expected token %v but got %v", expectedToken, resp.Token)
	}
	if resp.MasterToken != expectedMasterToken {
		t.Fatalf("Expected master token %v but got %v", expectedMasterToken, resp.MasterToken)
	}
	if resp.SessionInfo.DatabaseName != sc.cfg.Database {
		t.Fatalf("Expected database %v but got %v", sc.cfg.Database, resp.SessionInfo.DatabaseName)
	}
	if resp.SessionInfo.WarehouseName != sc.cfg.Warehouse {
		t.Fatalf("Expected warehouse %v but got %v", sc.cfg.Warehouse, resp.SessionInfo.WarehouseName)
	}
	if resp.SessionInfo.RoleName != sc.cfg.Role {
		t.Fatalf("Expected role %v but got %v", sc.cfg.Role, resp.SessionInfo.RoleName)
	}
	if resp.SessionInfo.SchemaName != sc.cfg.Schema {
		t.Fatalf("Expected schema %v but got %v", sc.cfg.Schema, resp.SessionInfo.SchemaName)
	}
}

func TestUnitAuthenticate(t *testing.T) {
	var err error
	var driverErr *SnowflakeError
	var ok bool

	ta := getSimpleTokenAccessor()
	sc := getDefaultSnowflakeConn()
	sr := &snowflakeRestful{
		FuncPostAuth:  postAuthFailServiceIssue,
		TokenAccessor: ta,
	}
	sc.rest = sr

	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrCodeServiceUnavailable {
		t.Fatalf("Snowflake error is expected. err: %v", driverErr)
	}
	sr.FuncPostAuth = postAuthFailWrongAccount
	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrCodeFailedToConnect {
		t.Fatalf("Snowflake error is expected. err: %v", driverErr)
	}
	sr.FuncPostAuth = postAuthFailUnknown
	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrFailedToAuth {
		t.Fatalf("Snowflake error is expected. err: %v", driverErr)
	}
	ta.SetTokens("bad-token", "bad-master-token", 1)
	sr.FuncPostAuth = postAuthSuccessWithErrorCode
	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err == nil {
		t.Fatal("should have failed.")
	}
	newToken, newMasterToken, newSessionID := ta.GetTokens()
	if newToken != "" || newMasterToken != "" || newSessionID != -1 {
		t.Fatalf("failed auth should have reset tokens: %v %v %v", newToken, newMasterToken, newSessionID)
	}
	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != 98765 {
		t.Fatalf("Snowflake error is expected. err: %v", driverErr)
	}
	ta.SetTokens("bad-token", "bad-master-token", 1)
	sr.FuncPostAuth = postAuthSuccessWithInvalidErrorCode
	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err == nil {
		t.Fatal("should have failed.")
	}
	oldToken, oldMasterToken, oldSessionID := ta.GetTokens()
	if oldToken != "" || oldMasterToken != "" || oldSessionID != -1 {
		t.Fatalf("failed auth should have reset tokens: %v %v %v", oldToken, oldMasterToken, oldSessionID)
	}
	sr.FuncPostAuth = postAuthSuccess
	var resp *authResponseMain
	resp, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("failed to auth. err: %v", err)
	}
	if resp.SessionInfo.DatabaseName != "dbn" {
		t.Fatalf("failed to get response from auth")
	}
	newToken, newMasterToken, newSessionID = ta.GetTokens()
	if newToken == oldToken {
		t.Fatalf("new token was not set: %v", newToken)
	}
	if newMasterToken == oldMasterToken {
		t.Fatalf("new master token was not set: %v", newMasterToken)
	}
	if newSessionID == oldSessionID {
		t.Fatalf("new session id was not set: %v", newSessionID)
	}
}

func TestUnitAuthenticateSaml(t *testing.T) {
	var err error
	sr := &snowflakeRestful{
		Protocol:         "https",
		Host:             "abc.com",
		Port:             443,
		FuncPostAuthSAML: postAuthSAMLAuthSuccess,
		FuncPostAuthOKTA: postAuthOKTASuccess,
		FuncGetSSO:       getSSOSuccess,
		FuncPostAuth:     postAuthCheckSAMLResponse,
		TokenAccessor:    getSimpleTokenAccessor(),
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Authenticator = AuthTypeOkta
	sc.cfg.OktaURL = &url.URL{
		Scheme: "https",
		Host:   "abc.com",
	}
	sc.rest = sr
	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	assertNilF(t, err, "failed to run.")
}

// Unit test for OAuth.
func TestUnitAuthenticateOAuth(t *testing.T) {
	var err error
	sr := &snowflakeRestful{
		FuncPostAuth:  postAuthCheckOAuth,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Token = "oauthToken"
	sc.cfg.Authenticator = AuthTypeOAuth
	sc.rest = sr
	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}
}

func TestUnitAuthenticatePasscode(t *testing.T) {
	var err error
	sr := &snowflakeRestful{
		FuncPostAuth:  postAuthCheckPasscode,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Passcode = "987654321"
	sc.rest = sr

	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}
	sr.FuncPostAuth = postAuthCheckPasscodeInPassword
	sc.rest = sr
	sc.cfg.PasscodeInPassword = true
	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}
}

// Test JWT function in the local environment against the validation function in go
func TestUnitAuthenticateJWT(t *testing.T) {
	var err error

	sr := &snowflakeRestful{
		FuncPostAuth:  postAuthCheckJWTToken,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Authenticator = AuthTypeJwt
	sc.cfg.JWTExpireTimeout = defaultJWTTimeout
	sc.cfg.PrivateKey = testPrivKey
	sc.rest = sr

	// A valid JWT token should pass
	if _, err = authenticate(context.Background(), sc, []byte{}, []byte{}); err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}

	// An invalid JWT token should not pass
	invalidPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Error(err)
	}
	sc.cfg.PrivateKey = invalidPrivateKey
	if _, err = authenticate(context.Background(), sc, []byte{}, []byte{}); err == nil {
		t.Fatalf("invalid token passed")
	}
}

func TestUnitAuthenticateUsernamePasswordMfa(t *testing.T) {
	var err error
	sr := &snowflakeRestful{
		FuncPostAuth:  postAuthCheckUsernamePasswordMfa,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Authenticator = AuthTypeUsernamePasswordMFA
	sc.cfg.ClientRequestMfaToken = ConfigBoolTrue
	sc.rest = sr
	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}

	sr.FuncPostAuth = postAuthCheckUsernamePasswordMfaToken
	sc.cfg.MfaToken = "mockedMfaToken"
	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}

	sr.FuncPostAuth = postAuthCheckUsernamePasswordMfaFailed
	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestUnitAuthenticateWithConfigMFA(t *testing.T) {
	var err error
	sr := &snowflakeRestful{
		FuncPostAuth:  postAuthCheckUsernamePasswordMfa,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Authenticator = AuthTypeUsernamePasswordMFA
	sc.cfg.ClientRequestMfaToken = ConfigBoolTrue
	sc.rest = sr
	sc.ctx = context.Background()
	err = authenticateWithConfig(sc)
	if err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}
}

func TestUnitAuthenticateWithConfigOkta(t *testing.T) {
	var err error
	sr := &snowflakeRestful{
		Protocol:         "https",
		Host:             "abc.com",
		Port:             443,
		FuncPostAuthSAML: postAuthSAMLAuthSuccess,
		FuncPostAuthOKTA: postAuthOKTASuccess,
		FuncGetSSO:       getSSOSuccess,
		FuncPostAuth:     postAuthCheckSAMLResponse,
		TokenAccessor:    getSimpleTokenAccessor(),
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Authenticator = AuthTypeOkta
	sc.cfg.OktaURL = &url.URL{
		Scheme: "https",
		Host:   "abc.com",
	}
	sc.rest = sr
	sc.ctx = context.Background()

	err = authenticateWithConfig(sc)
	assertNilE(t, err, "expected to have no error.")

	sr.FuncPostAuthSAML = postAuthSAMLError
	err = authenticateWithConfig(sc)
	assertNotNilF(t, err, "should have failed at FuncPostAuthSAML.")
	assertEqualE(t, err.Error(), "failed to get SAML response")
}

func TestUnitAuthenticateWithConfigExternalBrowser(t *testing.T) {
	var err error
	sr := &snowflakeRestful{
		FuncPostAuthSAML: postAuthSAMLError,
		TokenAccessor:    getSimpleTokenAccessor(),
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Authenticator = AuthTypeExternalBrowser
	sc.cfg.ExternalBrowserTimeout = defaultExternalBrowserTimeout
	sc.rest = sr
	sc.ctx = context.Background()
	err = authenticateWithConfig(sc)
	assertNotNilF(t, err, "should have failed at FuncPostAuthSAML.")
	assertEqualE(t, err.Error(), "failed to get SAML response")
}

func TestUnitAuthenticateExternalBrowser(t *testing.T) {
	var err error
	sr := &snowflakeRestful{
		FuncPostAuth:  postAuthCheckExternalBrowser,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Authenticator = AuthTypeExternalBrowser
	sc.cfg.ClientStoreTemporaryCredential = ConfigBoolTrue
	sc.rest = sr
	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}

	sr.FuncPostAuth = postAuthCheckExternalBrowserToken
	sc.cfg.IDToken = "mockedIDToken"
	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err != nil {
		t.Fatalf("failed to run. err: %v", err)
	}

	sr.FuncPostAuth = postAuthCheckExternalBrowserFailed
	_, err = authenticate(context.Background(), sc, []byte{}, []byte{})
	if err == nil {
		t.Fatal("should have failed")
	}
}

// To run this test you need to set environment variables in parameters.json to a user with MFA authentication enabled
// Set any other snowflake_test variables needed for database, schema, role for this user
func TestUsernamePasswordMfaCaching(t *testing.T) {
	t.Skip("manual test for MFA token caching")

	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal("Failed to parse dsn")
	}
	// connect with MFA authentication
	user := os.Getenv("SNOWFLAKE_TEST_MFA_USER")
	password := os.Getenv("SNOWFLAKE_TEST_MFA_PASSWORD")
	config.User = user
	config.Password = password
	config.Authenticator = AuthTypeUsernamePasswordMFA
	if runtime.GOOS == "linux" {
		config.ClientRequestMfaToken = ConfigBoolTrue
	}
	connector := NewConnector(SnowflakeDriver{}, *config)
	db := sql.OpenDB(connector)
	for i := 0; i < 3; i++ {
		// should only be prompted to authenticate first time around.
		_, err := db.Query("select current_user()")
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestUsernamePasswordMfaCachingWithPasscode(t *testing.T) {
	t.Skip("manual test for MFA token caching")

	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal("Failed to parse dsn")
	}
	// connect with MFA authentication
	user := os.Getenv("SNOWFLAKE_TEST_MFA_USER")
	password := os.Getenv("SNOWFLAKE_TEST_MFA_PASSWORD")
	config.User = user
	config.Password = password
	config.Passcode = "" // fill with your passcode from DUO app
	config.Authenticator = AuthTypeUsernamePasswordMFA
	if runtime.GOOS == "linux" {
		config.ClientRequestMfaToken = ConfigBoolTrue
	}
	connector := NewConnector(SnowflakeDriver{}, *config)
	db := sql.OpenDB(connector)
	for i := 0; i < 3; i++ {
		// should only be prompted to authenticate first time around.
		_, err := db.Query("select current_user()")
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestUsernamePasswordMfaCachingWithPasscodeInPassword(t *testing.T) {
	t.Skip("manual test for MFA token caching")

	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal("Failed to parse dsn")
	}
	// connect with MFA authentication
	user := os.Getenv("SNOWFLAKE_TEST_MFA_USER")
	password := os.Getenv("SNOWFLAKE_TEST_MFA_PASSWORD")
	config.User = user
	config.Password = password + "" // fill with your passcode from DUO app
	config.PasscodeInPassword = true
	connector := NewConnector(SnowflakeDriver{}, *config)
	db := sql.OpenDB(connector)
	for i := 0; i < 3; i++ {
		// should only be prompted to authenticate first time around.
		_, err := db.Query("select current_user()")
		if err != nil {
			t.Fatal(err)
		}
	}
}

// To run this test you need to set environment variables in parameters.json to a user with MFA authentication enabled
// Set any other snowflake_test variables needed for database, schema, role for this user
func TestDisableUsernamePasswordMfaCaching(t *testing.T) {
	t.Skip("manual test for disabling MFA token caching")

	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal("Failed to parse dsn")
	}
	// connect with MFA authentication
	user := os.Getenv("SNOWFLAKE_TEST_MFA_USER")
	password := os.Getenv("SNOWFLAKE_TEST_MFA_PASSWORD")
	config.User = user
	config.Password = password
	config.Authenticator = AuthTypeUsernamePasswordMFA
	// disable MFA token caching
	config.ClientRequestMfaToken = ConfigBoolFalse
	connector := NewConnector(SnowflakeDriver{}, *config)
	db := sql.OpenDB(connector)
	for i := 0; i < 3; i++ {
		// should be prompted to authenticate 3 times.
		_, err := db.Query("select current_user()")
		if err != nil {
			t.Fatal(err)
		}
	}
}

// To run this test you need to set SNOWFLAKE_TEST_EXT_BROWSER_USER environment variable to an external browser user
// Set any other snowflake_test variables needed for database, schema, role for this user
func TestExternalBrowserCaching(t *testing.T) {
	t.Skip("manual test for external browser token caching")

	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal("Failed to parse dsn")
	}
	// connect with external browser authentication
	user := os.Getenv("SNOWFLAKE_TEST_EXT_BROWSER_USER")
	config.User = user
	config.Authenticator = AuthTypeExternalBrowser
	if runtime.GOOS == "linux" {
		config.ClientStoreTemporaryCredential = ConfigBoolTrue
	}
	connector := NewConnector(SnowflakeDriver{}, *config)
	db := sql.OpenDB(connector)
	for i := 0; i < 3; i++ {
		// should only be prompted to authenticate first time around.
		_, err := db.Query("select current_user()")
		if err != nil {
			t.Fatal(err)
		}
	}
}

// To run this test you need to set SNOWFLAKE_TEST_EXT_BROWSER_USER environment variable to an external browser user
// Set any other snowflake_test variables needed for database, schema, role for this user
func TestDisableExternalBrowserCaching(t *testing.T) {
	t.Skip("manual test for disabling external browser token caching")

	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal("Failed to parse dsn")
	}
	// connect with external browser authentication
	user := os.Getenv("SNOWFLAKE_TEST_EXT_BROWSER_USER")
	config.User = user
	config.Authenticator = AuthTypeExternalBrowser
	// disable external browser token caching
	config.ClientStoreTemporaryCredential = ConfigBoolFalse
	connector := NewConnector(SnowflakeDriver{}, *config)
	db := sql.OpenDB(connector)
	for i := 0; i < 3; i++ {
		// should be prompted to authenticate 3 times.
		_, err := db.Query("select current_user()")
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestOktaRetryWithNewToken(t *testing.T) {
	expectedMasterToken := "m"
	expectedToken := "t"
	expectedMfaToken := "mockedMfaToken"
	expectedDatabaseName := "dbn"

	sr := &snowflakeRestful{
		Protocol:         "https",
		Host:             "abc.com",
		Port:             443,
		FuncPostAuthSAML: postAuthSAMLAuthSuccess,
		FuncPostAuthOKTA: postAuthOKTASuccess,
		FuncGetSSO:       getSSOSuccess,
		FuncPostAuth:     postAuthOktaWithNewToken,
		TokenAccessor:    getSimpleTokenAccessor(),
	}
	sc := getDefaultSnowflakeConn()
	sc.cfg.Authenticator = AuthTypeOkta
	sc.cfg.OktaURL = &url.URL{
		Scheme: "https",
		Host:   "abc.com",
	}
	sc.rest = sr
	sc.ctx = context.Background()

	authResponse, err := authenticate(context.Background(), sc, []byte{0x12, 0x34}, []byte{0x56, 0x78})
	assertNilF(t, err, "should not have failed to run authenticate()")
	assertEqualF(t, authResponse.MasterToken, expectedMasterToken)
	assertEqualF(t, authResponse.Token, expectedToken)
	assertEqualF(t, authResponse.MfaToken, expectedMfaToken)
	assertEqualF(t, authResponse.SessionInfo.DatabaseName, expectedDatabaseName)
}

func TestContextPropagatedToAuthWhenUsingOpen(t *testing.T) {
	db, err := sql.Open("snowflake", dsn)
	assertNilF(t, err)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err = db.QueryContext(ctx, "SELECT 1")
	assertNotNilF(t, err)
	assertStringContainsE(t, err.Error(), "context deadline exceeded")
	cancel()
}

func TestContextPropagatedToAuthWhenUsingOpenDB(t *testing.T) {
	cfg, err := ParseDSN(dsn)
	assertNilF(t, err)
	connector := NewConnector(&SnowflakeDriver{}, *cfg)
	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err = db.QueryContext(ctx, "SELECT 1")
	assertNotNilF(t, err)
	assertStringContainsE(t, err.Error(), "context deadline exceeded")
	cancel()
}

func TestPatSuccessfulFlow(t *testing.T) {
	cfg := wiremock.connectionConfig()
	cfg.Authenticator = AuthTypePat
	cfg.Token = "some PAT"
	testPatSuccessfulFlow(t, cfg)
}

func testPatSuccessfulFlow(t *testing.T, cfg *Config) {
	skipOnJenkins(t, "wiremock is not enabled")
	enableExperimentalAuth(t)
	wiremock.registerMappings(t,
		wiremockMapping{filePath: "auth/pat/successful_flow.json"},
		wiremockMapping{filePath: "select1.json", params: map[string]string{
			"%AUTHORIZATION_HEADER%": "Snowflake Token=\\\"session token\\\""},
		},
	)
	connector := NewConnector(SnowflakeDriver{}, *cfg)
	db := sql.OpenDB(connector)
	rows, err := db.Query("SELECT 1")
	assertNilF(t, err)
	var v int
	assertTrueE(t, rows.Next())
	assertNilF(t, rows.Scan(&v))
	assertEqualE(t, v, 1)
}

func enableExperimentalAuth(t *testing.T) {
	err := os.Setenv("ENABLE_EXPERIMENTAL_AUTHENTICATION", "true")
	assertNilF(t, err)
}

func TestPatSuccessfulFlowWithPatAsPasswordWithPatAuthenticator(t *testing.T) {
	cfg := wiremock.connectionConfig()
	cfg.Authenticator = AuthTypePat
	cfg.Password = "some PAT"
	testPatSuccessfulFlow(t, cfg)
}

func TestPatInvalidToken(t *testing.T) {
	skipOnJenkins(t, "wiremock is not enabled")
	enableExperimentalAuth(t)
	wiremock.registerMappings(t,
		wiremockMapping{filePath: "auth/pat/invalid_token.json"},
	)
	cfg := wiremock.connectionConfig()
	cfg.Authenticator = AuthTypePat
	cfg.Token = "some PAT"
	connector := NewConnector(SnowflakeDriver{}, *cfg)
	db := sql.OpenDB(connector)
	_, err := db.Query("SELECT 1")
	assertNotNilF(t, err)
	var se *SnowflakeError
	assertTrueF(t, errors.As(err, &se))
	assertEqualE(t, se.Number, 394400)
	assertEqualE(t, se.Message, "Programmatic access token is invalid.")
}
