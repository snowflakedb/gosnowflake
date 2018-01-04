// Copyright (c) 2017-2018 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestUnitPostBackURL(t *testing.T) {
	c := `<html><form id="1" action="https&#x3a;&#x2f;&#x2f;abc.com&#x2f;"></form></html>`
	urlp, err := postBackURL([]byte(c))
	if err != nil {
		t.Fatalf("failed to get URL. err: %v, %v", err, c)
	}
	if urlp != "https://abc.com/" {
		t.Errorf("failed to get URL. got: %v, %v", urlp, c)
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

type tcIsPrefixEqual struct {
	url1   string
	url2   string
	result bool
	err    error
}

func TestUnitIsPrefixEqual(t *testing.T) {
	testcases := []tcIsPrefixEqual{
		{url1: "https://abc.com/", url2: "https://abc.com", result: true},
		{url1: "https://def.com/", url2: "https://abc.com", result: false},
		{url1: "http://def.com", url2: "https://def.com", result: false},
		{url1: "afdafdafadfs", url2: "https://def.com", result: false},
		{url1: "http://def.com", url2: "afdafafd", result: false},
		{url1: "https://abc.com", url2: "https://abc.com:443/", result: true},
	}
	for _, test := range testcases {
		r, err := isPrefixEqual(test.url1, test.url2)
		if test.err != nil {
			if err == nil {
				t.Errorf("should have failed. url1: %v, url2: %v, got: %v, expected err: %v", test.url1, test.url2, r, test.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("failed. url1: %v, url2: %v, expected: %v, err: %v", test.url1, test.url2, test.result, err)
		} else {
			if r && !test.result || !r && test.result {
				t.Errorf("failed. url1: %v, url2: %v, expected: %v, got: %v", test.url1, test.url2, test.result, r)
			}
		}
	}
}

func getTestError(_ context.Context, _ *snowflakeRestful, _ string, _ map[string]string, _ time.Duration) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, errors.New("failed to run post method")
}

func getTestAppBadGatewayError(_ context.Context, _ *snowflakeRestful, _ string, _ map[string]string, _ time.Duration) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func getTestHTMLSuccess(_ context.Context, _ *snowflakeRestful, _ string, _ map[string]string, _ time.Duration) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte("<htm></html>")},
	}, nil
}

func TestUnitPostAuthSAML(t *testing.T) {
	sr := &snowflakeRestful{
		FuncPost: postTestError,
	}
	var err error
	_, err = postAuthSAML(sr, make(map[string]string), []byte{}, 0)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPost = postTestAppBadGatewayError
	_, err = postAuthSAML(sr, make(map[string]string), []byte{}, 0)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	_, err = postAuthSAML(sr, make(map[string]string), []byte{0x12, 0x34}, 0)
	if err == nil {
		t.Fatalf("should have failed to post")
	}
}

func TestUnitPostAuthOKTA(t *testing.T) {
	sr := &snowflakeRestful{
		FuncPost: postTestError,
	}
	var err error
	_, err = postAuthOKTA(sr, make(map[string]string), []byte{}, "hahah", 0)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPost = postTestAppBadGatewayError
	_, err = postAuthOKTA(sr, make(map[string]string), []byte{}, "hahah", 0)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	_, err = postAuthOKTA(sr, make(map[string]string), []byte{0x12, 0x34}, "haha", 0)
	if err == nil {
		t.Fatal("should have failed to run post request after the renewal")
	}
}

func TestUnitGetSSO(t *testing.T) {
	sr := &snowflakeRestful{
		FuncGet: getTestError,
	}
	var err error
	_, err = getSSO(sr, &url.Values{}, make(map[string]string), "hahah", 0)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncGet = getTestAppBadGatewayError
	_, err = getSSO(sr, &url.Values{}, make(map[string]string), "hahah", 0)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncGet = getTestHTMLSuccess
	_, err = getSSO(sr, &url.Values{}, make(map[string]string), "hahah", 0)
	if err != nil {
		t.Fatalf("failed to get HTML content. err: %v", err)
	}
}

func postAuthSAMLError(_ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{}, errors.New("failed to get SAML response")
}

func postAuthSAMLAuthFail(_ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: false,
		Message: "SAML auth failed",
	}, nil
}

func postAuthSAMLAuthSuccessButInvalidURL(_ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: true,
		Message: "",
		Data: authResponseMain{
			TokenURL: "https://1abc.com/token",
			SSOURL:   "https://2abc.com/sso",
		},
	}, nil
}

func postAuthSAMLAuthSuccess(_ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: true,
		Message: "",
		Data: authResponseMain{
			TokenURL: "https://abc.com/token",
			SSOURL:   "https://abc.com/sso",
		},
	}, nil
}

func postAuthOKTAError(_ *snowflakeRestful, _ map[string]string, _ []byte, _ string, _ time.Duration) (*authOKTAResponse, error) {
	return &authOKTAResponse{}, errors.New("failed to get SAML response")
}

func postAuthOKTASuccess(_ *snowflakeRestful, _ map[string]string, _ []byte, _ string, _ time.Duration) (*authOKTAResponse, error) {
	return &authOKTAResponse{}, nil
}

func getSSOError(_ *snowflakeRestful, _ *url.Values, _ map[string]string, _ string, _ time.Duration) ([]byte, error) {
	return []byte{}, errors.New("failed to get SSO html")
}

func getSSOSuccessButInvalidURL(_ *snowflakeRestful, _ *url.Values, _ map[string]string, _ string, _ time.Duration) ([]byte, error) {
	return []byte(`<html><form id="1"/></html>`), nil
}

func getSSOSuccess(_ *snowflakeRestful, _ *url.Values, _ map[string]string, _ string, _ time.Duration) ([]byte, error) {
	return []byte(`<html><form id="1" action="https&#x3a;&#x2f;&#x2f;abc.com&#x2f;"></form></html>`), nil
}

func TestUnitAuthenticateBySAML(t *testing.T) {
	authenticator := "https://abc.com/"
	application := "testapp"
	account := "testaccount"
	user := "u"
	password := "p"
	sr := &snowflakeRestful{
		Protocol:         "https",
		Host:             "abc.com",
		Port:             443,
		FuncPostAuthSAML: postAuthSAMLError,
	}
	var err error
	_, err = authenticateBySAML(sr, authenticator, application, account, user, password)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPostAuthSAML = postAuthSAMLAuthFail
	_, err = authenticateBySAML(sr, authenticator, application, account, user, password)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPostAuthSAML = postAuthSAMLAuthSuccessButInvalidURL
	_, err = authenticateBySAML(sr, authenticator, application, account, user, password)
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("should be snowflake error. err: %v", err)
	}
	if driverErr.Number != ErrCodeIdpConnectionError {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrCodeIdpConnectionError, driverErr.Number)
	}
	sr.FuncPostAuthSAML = postAuthSAMLAuthSuccess
	sr.FuncPostAuthOKTA = postAuthOKTAError
	_, err = authenticateBySAML(sr, authenticator, application, account, user, password)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPostAuthOKTA = postAuthOKTASuccess
	sr.FuncGetSSO = getSSOError
	_, err = authenticateBySAML(sr, authenticator, application, account, user, password)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncGetSSO = getSSOSuccessButInvalidURL
	_, err = authenticateBySAML(sr, authenticator, application, account, user, password)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncGetSSO = getSSOSuccess
	_, err = authenticateBySAML(sr, authenticator, application, account, user, password)
	if err != nil {
		t.Fatalf("failed. err: %v", err)
	}
}
