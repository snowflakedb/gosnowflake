// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func TestUnitPostBackURL(t *testing.T) {
	c := `<html><form id="1" action="https&#x3a;&#x2f;&#x2f;abc.com&#x2f;"></form></html>`
	pbURL, err := postBackURL([]byte(c))
	if err != nil {
		t.Fatalf("failed to get URL. err: %v, %v", err, c)
	}
	if pbURL.String() != "https://abc.com/" {
		t.Errorf("failed to get URL. got: %v, %v", pbURL, c)
	}
	c = `<html></html>`
	_, err = postBackURL([]byte(c))
	if err == nil {
		t.Fatalf("should have failed")
	}
	c = `<html><form id="1"/></html>`
	_, err = postBackURL([]byte(c))
	if err == nil {
		t.Fatalf("should have failed")
	}
	c = `<html><form id="1" action="https&#x3a;&#x2f;&#x2f;abc.com&#x2f;/></html>`
	_, err = postBackURL([]byte(c))
	if err == nil {
		t.Fatalf("should have failed")
	}
}

func getTestError(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ time.Duration) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, errors.New("failed to run post method")
}

func getTestAppBadGatewayError(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ time.Duration) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func getTestHTMLSuccess(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ time.Duration) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte("<htm></html>")},
	}, nil
}

func TestUnitPostAuthSAML(t *testing.T) {
	sr := &snowflakeRestful{
		FuncPost:      postTestError,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	var err error
	_, err = postAuthSAML(context.Background(), sr, make(map[string]string), []byte{}, 0)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPost = postTestAppBadGatewayError
	_, err = postAuthSAML(context.Background(), sr, make(map[string]string), []byte{}, 0)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	_, err = postAuthSAML(context.Background(), sr, make(map[string]string), []byte{0x12, 0x34}, 0)
	if err == nil {
		t.Fatalf("should have failed to post")
	}
}

func TestUnitPostAuthOKTA(t *testing.T) {
	sr := &snowflakeRestful{
		FuncPost:      postTestError,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	var err error
	_, err = postAuthOKTA(context.Background(), sr, make(map[string]string), []byte{}, "hahah", 0)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPost = postTestAppBadGatewayError
	_, err = postAuthOKTA(context.Background(), sr, make(map[string]string), []byte{}, "hahah", 0)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	_, err = postAuthOKTA(context.Background(), sr, make(map[string]string), []byte{0x12, 0x34}, "haha", 0)
	if err == nil {
		t.Fatal("should have failed to run post request after the renewal")
	}
}

func TestUnitGetSSO(t *testing.T) {
	sr := &snowflakeRestful{
		FuncGet:       getTestError,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	var err error
	_, err = getSSO(context.Background(), sr, &url.Values{}, make(map[string]string), "hahah", 0)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncGet = getTestAppBadGatewayError
	_, err = getSSO(context.Background(), sr, &url.Values{}, make(map[string]string), "hahah", 0)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncGet = getTestHTMLSuccess
	_, err = getSSO(context.Background(), sr, &url.Values{}, make(map[string]string), "hahah", 0)
	if err != nil {
		t.Fatalf("failed to get HTML content. err: %v", err)
	}
	_, err = getSSO(context.Background(), sr, &url.Values{}, make(map[string]string), "invalid!@url$%^", 0)
	if err == nil {
		t.Fatal("should have failed to parse URL.")
	}
}

func postAuthSAMLError(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{}, errors.New("failed to get SAML response")
}

func postAuthSAMLAuthFail(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: false,
		Message: "SAML auth failed",
	}, nil
}

func postAuthSAMLAuthFailWithCode(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: false,
		Code:    strconv.Itoa(ErrCodeIdpConnectionError),
		Message: "SAML auth failed",
	}, nil
}

func postAuthSAMLAuthSuccessButInvalidURL(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: true,
		Message: "",
		Data: authResponseMain{
			TokenURL: "https://1abc.com/token",
			SSOURL:   "https://2abc.com/sso",
		},
	}, nil
}

func postAuthSAMLAuthSuccessButInvalidTokenURL(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: true,
		Message: "",
		Data: authResponseMain{
			TokenURL: "invalid!@url$%^",
			SSOURL:   "https://abc.com/sso",
		},
	}, nil
}

func postAuthSAMLAuthSuccessButInvalidSSOURL(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: true,
		Message: "",
		Data: authResponseMain{
			TokenURL: "https://abc.com/token",
			SSOURL:   "invalid!@url$%^",
		},
	}, nil
}

func postAuthSAMLAuthSuccess(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: true,
		Message: "",
		Data: authResponseMain{
			TokenURL: "https://abc.com/token",
			SSOURL:   "https://abc.com/sso",
		},
	}, nil
}

func postAuthOKTAError(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ string, _ time.Duration) (*authOKTAResponse, error) {
	return &authOKTAResponse{}, errors.New("failed to get SAML response")
}

func postAuthOKTASuccess(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ string, _ time.Duration) (*authOKTAResponse, error) {
	return &authOKTAResponse{}, nil
}

func getSSOError(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ string, _ time.Duration) ([]byte, error) {
	return []byte{}, errors.New("failed to get SSO html")
}

func getSSOSuccessButInvalidURL(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ string, _ time.Duration) ([]byte, error) {
	return []byte(`<html><form id="1"/></html>`), nil
}

func getSSOSuccess(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ string, _ time.Duration) ([]byte, error) {
	return []byte(`<html><form id="1" action="https&#x3a;&#x2f;&#x2f;abc.com&#x2f;"></form></html>`), nil
}

func getSSOSuccessButWrongPrefixURL(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ string, _ time.Duration) ([]byte, error) {
	return []byte(`<html><form id="1" action="https&#x3a;&#x2f;&#x2f;1abc.com&#x2f;"></form></html>`), nil
}

func TestUnitAuthenticateBySAML(t *testing.T) {
	authenticator := &url.URL{
		Scheme: "https",
		Host:   "abc.com",
	}
	application := "testapp"
	account := "testaccount"
	user := "u"
	password := "p"
	sr := &snowflakeRestful{
		Protocol:         "https",
		Host:             "abc.com",
		Port:             443,
		FuncPostAuthSAML: postAuthSAMLError,
		TokenAccessor:    getSimpleTokenAccessor(),
	}
	var err error
	_, err = authenticateBySAML(context.Background(), sr, authenticator, application, account, user, password, ConfigBoolFalse)
	assertNotNilF(t, err, "should have failed at FuncPostAuthSAML.")
	assertEqualE(t, err.Error(), "failed to get SAML response")

	sr.FuncPostAuthSAML = postAuthSAMLAuthFail
	_, err = authenticateBySAML(context.Background(), sr, authenticator, application, account, user, password, ConfigBoolFalse)
	assertNotNilF(t, err, "should have failed at FuncPostAuthSAML.")
	assertEqualE(t, err.Error(), "strconv.Atoi: parsing \"\": invalid syntax")

	sr.FuncPostAuthSAML = postAuthSAMLAuthFailWithCode
	_, err = authenticateBySAML(context.Background(), sr, authenticator, application, account, user, password, ConfigBoolFalse)
	assertNotNilF(t, err, "should have failed at FuncPostAuthSAML.")
	driverErr, ok := err.(*SnowflakeError)
	assertTrueF(t, ok, "should be a SnowflakeError")
	assertEqualE(t, driverErr.Number, ErrCodeIdpConnectionError)

	sr.FuncPostAuthSAML = postAuthSAMLAuthSuccessButInvalidURL
	_, err = authenticateBySAML(context.Background(), sr, authenticator, application, account, user, password, ConfigBoolFalse)
	assertNotNilF(t, err, "should have failed at FuncPostAuthSAML.")
	driverErr, ok = err.(*SnowflakeError)
	assertTrueF(t, ok, "should be a SnowflakeError")
	assertEqualE(t, driverErr.Number, ErrCodeIdpConnectionError)

	sr.FuncPostAuthSAML = postAuthSAMLAuthSuccessButInvalidTokenURL
	_, err = authenticateBySAML(context.Background(), sr, authenticator, application, account, user, password, ConfigBoolFalse)
	assertNotNilF(t, err, "should have failed at FuncPostAuthSAML.")
	assertEqualE(t, err.Error(), "failed to parse token URL. invalid!@url$%^")

	sr.FuncPostAuthSAML = postAuthSAMLAuthSuccessButInvalidSSOURL
	_, err = authenticateBySAML(context.Background(), sr, authenticator, application, account, user, password, ConfigBoolFalse)
	assertNotNilF(t, err, "should have failed at FuncPostAuthSAML.")
	assertEqualE(t, err.Error(), "failed to parse SSO URL. invalid!@url$%^")

	sr.FuncPostAuthSAML = postAuthSAMLAuthSuccess
	sr.FuncPostAuthOKTA = postAuthOKTAError
	_, err = authenticateBySAML(context.Background(), sr, authenticator, application, account, user, password, ConfigBoolFalse)
	assertNotNilF(t, err, "should have failed at FuncPostAuthOKTA.")
	assertEqualE(t, err.Error(), "failed to get SAML response")

	sr.FuncPostAuthOKTA = postAuthOKTASuccess
	sr.FuncGetSSO = getSSOError
	_, err = authenticateBySAML(context.Background(), sr, authenticator, application, account, user, password, ConfigBoolFalse)
	assertNotNilF(t, err, "should have failed at FuncGetSSO.")
	assertEqualE(t, err.Error(), "failed to get SSO html")

	sr.FuncGetSSO = getSSOSuccessButInvalidURL
	_, err = authenticateBySAML(context.Background(), sr, authenticator, application, account, user, password, ConfigBoolFalse)
	assertNotNilF(t, err, "should have failed at FuncGetSSO.")
	assertHasPrefixE(t, err.Error(), "failed to find action field in HTML response")

	sr.FuncGetSSO = getSSOSuccess
	_, err = authenticateBySAML(context.Background(), sr, authenticator, application, account, user, password, ConfigBoolFalse)
	assertNilF(t, err, "should have succeeded at FuncGetSSO.")

	sr.FuncGetSSO = getSSOSuccessButWrongPrefixURL
	_, err = authenticateBySAML(context.Background(), sr, authenticator, application, account, user, password, ConfigBoolFalse)
	assertNotNilF(t, err, "should have failed at FuncGetSSO.")
	driverErr, ok = err.(*SnowflakeError)
	assertTrueF(t, ok, "should be a SnowflakeError")
	assertEqualE(t, driverErr.Number, ErrCodeSSOURLNotMatch)
}

func TestDisableSamlURLCheck(t *testing.T) {
	authenticator := &url.URL{
		Scheme: "https",
		Host:   "abc.com",
	}
	application := "testapp"
	account := "testaccount"
	user := "u"
	password := "p"
	sr := &snowflakeRestful{
		Protocol:         "https",
		Host:             "abc.com",
		Port:             443,
		FuncPostAuthSAML: postAuthSAMLAuthSuccess,
		FuncPostAuthOKTA: postAuthOKTASuccess,
		FuncGetSSO:       getSSOSuccessButWrongPrefixURL,
		TokenAccessor:    getSimpleTokenAccessor(),
	}
	var err error
	// Test for disabled SAML URL check
	_, err = authenticateBySAML(context.Background(), sr, authenticator, application, account, user, password, ConfigBoolTrue)
	assertNilF(t, err, "SAML URL check should have disabled.")

	// Test for enabled SAML URL check
	_, err = authenticateBySAML(context.Background(), sr, authenticator, application, account, user, password, ConfigBoolFalse)
	assertNotNilF(t, err, "should have failed at FuncGetSSO.")
	driverErr, ok := err.(*SnowflakeError)
	assertTrueF(t, ok, "should be a SnowflakeError")
	assertEqualE(t, driverErr.Number, ErrCodeSSOURLNotMatch)
}
