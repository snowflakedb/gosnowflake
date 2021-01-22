// Copyright (c) 2017-2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
)

func postTestError(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, errors.New("failed to run post method")
}

func postTestSuccessButInvalidJSON(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestAppBadGatewayError(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestAppForbiddenError(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusForbidden,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestAppUnexpectedError(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusInsufficientStorage,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestRenew(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	dd := &execResponseData{}
	er := &execResponse{
		Data:    *dd,
		Message: "",
		Code:    sessionExpiredCode,
		Success: true,
	}

	ba, err := json.Marshal(er)
	logger.Infof("encoded JSON: %v", ba)
	if err != nil {
		panic(err)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: ba},
	}, nil
}

func postTestAfterRenew(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	dd := &execResponseData{}
	er := &execResponse{
		Data:    *dd,
		Message: "",
		Code:    "",
		Success: true,
	}

	ba, err := json.Marshal(er)
	logger.Infof("encoded JSON: %v", ba)
	if err != nil {
		panic(err)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: ba},
	}, nil
}

func TestUnitPostQueryHelperError(t *testing.T) {
	sr := &snowflakeRestful{
		FuncPost:      postTestError,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	var err error
	var requestID uuid.UUID
	requestID = uuid.New()
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, requestID, &Config{})
	if err == nil {
		t.Fatalf("should have failed to post")
	}
	sr.FuncPost = postTestAppBadGatewayError
	requestID = uuid.New()
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, requestID, &Config{})
	if err == nil {
		t.Fatalf("should have failed to post")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	requestID = uuid.New()
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, requestID, &Config{})
	if err == nil {
		t.Fatalf("should have failed to post")
	}
}

func renewSessionTest(_ context.Context, _ *snowflakeRestful, _ time.Duration) error {
	return nil
}

func renewSessionTestError(_ context.Context, _ *snowflakeRestful, _ time.Duration) error {
	return errors.New("failed to renew session in tests")
}

func TestUnitPostQueryHelperUsesToken(t *testing.T) {
	accessor := getSimpleTokenAccessor()
	token := "token123"
	accessor.SetTokens(token, "", 0)

	var err error
	postQueryTest := func(_ context.Context, _ *snowflakeRestful, _ *url.Values, headers map[string]string, _ []byte, _ time.Duration, _ string) (*execResponse, error) {
		if headers[headerAuthorizationKey] != fmt.Sprintf(headerSnowflakeToken, token) {
			t.Fatalf("authorization key doesn't match, %v vs %v", headers[headerAuthorizationKey], fmt.Sprintf(headerSnowflakeToken, token))
		}
		dd := &execResponseData{}
		return &execResponse{
			Data:    *dd,
			Message: "",
			Code:    "0",
			Success: true,
		}, nil
	}
	sr := &snowflakeRestful{
		FuncPost:         postTestRenew,
		FuncPostQuery:    postQueryTest,
		FuncRenewSession: renewSessionTest,
		TokenAccessor:    accessor,
	}
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestUnitPostQueryHelperRenewSession(t *testing.T) {
	var err error
	origRequestID := uuid.New()
	postQueryTest := func(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration, requestID uuid.UUID, _ *Config) (*execResponse, error) {
		// ensure the same requestID is used after the session token is renewed.
		if requestID != origRequestID {
			t.Fatal("requestID doesn't match")
		}
		dd := &execResponseData{}
		return &execResponse{
			Data:    *dd,
			Message: "",
			Code:    "0",
			Success: true,
		}, nil
	}
	sr := &snowflakeRestful{
		FuncPost:         postTestRenew,
		FuncPostQuery:    postQueryTest,
		FuncRenewSession: renewSessionTest,
		TokenAccessor:    getSimpleTokenAccessor(),
	}

	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, origRequestID, &Config{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sr.FuncRenewSession = renewSessionTestError
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, origRequestID, &Config{})
	if err == nil {
		t.Fatal("should have failed to renew session")
	}
}

func TestUnitRenewRestfulSession(t *testing.T) {
	accessor := getSimpleTokenAccessor()
	oldToken, oldMasterToken, oldSessionId := "oldtoken", "oldmaster", 100
	accessor.SetTokens(oldToken, oldMasterToken, oldSessionId)

	newToken, newMasterToken, newSessionId := "newtoken", "newmaster", 200
	postTestSuccessWithNewTokens := func(_ context.Context, _ *snowflakeRestful, _ *url.URL, headers map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
		tr := &renewSessionResponse{
			Data: renewSessionResponseMain{
				SessionToken: newToken,
				MasterToken:  newMasterToken,
				SessionID:    newSessionId,
			},
			Message: "",
			Success: true,
		}
		ba, err := json.Marshal(tr)
		if err != nil {
			t.Fatalf("failed to serialize token response %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       &fakeResponseBody{body: ba},
		}, nil
	}

	sr := &snowflakeRestful{
		FuncPost:      postTestAfterRenew,
		TokenAccessor: accessor,
	}
	err := renewRestfulSession(context.Background(), sr, time.Second)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sr.FuncPost = postTestError
	err = renewRestfulSession(context.Background(), sr, time.Second)
	if err == nil {
		t.Fatal("should have failed to run post request after the renewal")
	}
	sr.FuncPost = postTestAppBadGatewayError
	err = renewRestfulSession(context.Background(), sr, time.Second)
	if err == nil {
		t.Fatal("should have failed to run post request after the renewal")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	err = renewRestfulSession(context.Background(), sr, time.Second)
	if err == nil {
		t.Fatal("should have failed to run post request after the renewal")
	}
	sr.FuncPost = postTestSuccessWithNewTokens
	err = renewRestfulSession(context.Background(), sr, time.Second)
	if err != nil {
		t.Fatal("should not have failed to run post request after the renewal")
	}
	token, masterToken, sessionId := accessor.GetTokens()
	if token != newToken {
		t.Fatalf("unexpected new token %v", token)
	}
	if masterToken != newMasterToken {
		t.Fatalf("unexpected new master token %v", masterToken)
	}
	if sessionId != newSessionId {
		t.Fatalf("unexpected new session id %v", sessionId)
	}
}

func TestUnitCloseSession(t *testing.T) {
	sr := &snowflakeRestful{
		FuncPost:      postTestAfterRenew,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	err := closeSession(context.Background(), sr, time.Second)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sr.FuncPost = postTestError
	err = closeSession(context.Background(), sr, time.Second)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
	sr.FuncPost = postTestAppBadGatewayError
	err = closeSession(context.Background(), sr, time.Second)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	err = closeSession(context.Background(), sr, time.Second)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
}

func TestUnitCancelQuery(t *testing.T) {
	sr := &snowflakeRestful{
		FuncPost: postTestAfterRenew,
	}
	ctx := context.Background()
	err := cancelQuery(ctx, sr, getOrGenerateRequestIDFromContext(ctx), time.Second)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sr.FuncPost = postTestError
	err = cancelQuery(ctx, sr, getOrGenerateRequestIDFromContext(ctx), time.Second)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
	sr.FuncPost = postTestAppBadGatewayError
	err = cancelQuery(context.Background(), sr, getOrGenerateRequestIDFromContext(ctx), time.Second)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	err = cancelQuery(context.Background(), sr, getOrGenerateRequestIDFromContext(ctx), time.Second)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
}
