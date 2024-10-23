// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func postTestError(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ currentTimeProvider, _ *Config) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, errors.New("failed to run post method")
}

func postAuthTestError(_ context.Context, _ *http.Client, _ *url.URL, _ map[string]string, _ bodyCreatorType, _ time.Duration, _ int) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, errors.New("failed to run post method")
}

func postTestSuccessButInvalidJSON(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ currentTimeProvider, _ *Config) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestAppBadGatewayError(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ currentTimeProvider, _ *Config) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postAuthTestAppBadGatewayError(_ context.Context, _ *http.Client, _ *url.URL, _ map[string]string, _ bodyCreatorType, _ time.Duration, _ int) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestAppForbiddenError(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ currentTimeProvider, _ *Config) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusForbidden,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postAuthTestAppForbiddenError(_ context.Context, _ *http.Client, _ *url.URL, _ map[string]string, _ bodyCreatorType, _ time.Duration, _ int) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusForbidden,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postAuthTestAppUnexpectedError(_ context.Context, _ *http.Client, _ *url.URL, _ map[string]string, _ bodyCreatorType, _ time.Duration, _ int) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusInsufficientStorage,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestQueryNotExecuting(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ currentTimeProvider, _ *Config) (*http.Response, error) {
	dd := &execResponseData{}
	er := &execResponse{
		Data:    *dd,
		Message: "",
		Code:    queryNotExecuting,
		Success: false,
	}
	ba, err := json.Marshal(er)
	if err != nil {
		panic(err)
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: ba},
	}, nil
}

func postTestRenew(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ currentTimeProvider, _ *Config) (*http.Response, error) {
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

func postAuthTestAfterRenew(_ context.Context, _ *http.Client, _ *url.URL, _ map[string]string, _ bodyCreatorType, _ time.Duration, _ int) (*http.Response, error) {
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

func postTestAfterRenew(_ context.Context, _ *snowflakeRestful, _ *url.URL, _ map[string]string, _ []byte, _ time.Duration, _ currentTimeProvider, _ *Config) (*http.Response, error) {
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

func cancelTestRetry(ctx context.Context, sr *snowflakeRestful, requestID UUID, timeout time.Duration) error {
	ctxRetry := getCancelRetry(ctx)
	u := url.URL{}
	reqByte, err := json.Marshal(make(map[string]string))
	if err != nil {
		return err
	}
	resp, err := sr.FuncPost(ctx, sr, &u, getHeaders(), reqByte, timeout, defaultTimeProvider, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		var respd cancelQueryResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			return err
		}
		if !respd.Success && respd.Code == queryNotExecuting && ctxRetry != 0 {
			return sr.FuncCancelQuery(context.WithValue(ctx, cancelRetry, ctxRetry-1), sr, requestID, timeout)
		}
		if ctxRetry == 0 {
			return nil
		}
	}
	return fmt.Errorf("cancel retry failed")
}

func TestUnitPostQueryHelperError(t *testing.T) {
	sr := &snowflakeRestful{
		FuncPost:      postTestError,
		TokenAccessor: getSimpleTokenAccessor(),
	}
	var err error
	requestID := NewUUID()
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, requestID, &Config{})
	if err == nil {
		t.Fatalf("should have failed to post")
	}
	sr.FuncPost = postTestAppBadGatewayError
	requestID = NewUUID()
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, requestID, &Config{})
	if err == nil {
		t.Fatalf("should have failed to post")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	requestID = NewUUID()
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, requestID, &Config{})
	if err == nil {
		t.Fatalf("should have failed to post")
	}
}

func TestUnitPostQueryHelperOnRenewSessionKeepsRequestIdButGeneratesNewRequestGuid(t *testing.T) {
	postCount := 0
	requestID := NewUUID()

	sr := &snowflakeRestful{
		FuncPost: func(ctx context.Context, restful *snowflakeRestful, url *url.URL, headers map[string]string, bytes []byte, duration time.Duration, provider currentTimeProvider, config *Config) (*http.Response, error) {
			assertEqualF(t, len((url.Query())[requestIDKey]), 1)
			assertEqualF(t, len((url.Query())[requestGUIDKey]), 1)
			return &http.Response{
				StatusCode: 200,
				Body:       &fakeResponseBody{body: []byte(`{"data":null,"code":"390112","message":"token expired for testing","success":false,"headers":null}`)},
			}, nil
		},
		FuncPostQuery: func(ctx context.Context, restful *snowflakeRestful, values *url.Values, headers map[string]string, bytes []byte, timeout time.Duration, uuid UUID, config *Config) (*execResponse, error) {
			assertEqualF(t, requestID.String(), uuid.String())
			assertEqualF(t, len((*values)[requestIDKey]), 1)
			assertEqualF(t, len((*values)[requestGUIDKey]), 1)
			if postCount == 0 {
				postCount++
				return postRestfulQueryHelper(ctx, restful, values, headers, bytes, timeout, uuid, config)
			}
			return nil, nil
		},
		FuncRenewSession: renewSessionTest,
		TokenAccessor:    getSimpleTokenAccessor(),
	}
	_, err := postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), make([]byte, 0), time.Second, requestID, nil)
	assertNilE(t, err)
}

func renewSessionTest(_ context.Context, _ *snowflakeRestful, _ time.Duration) error {
	return nil
}

func renewSessionTestError(_ context.Context, _ *snowflakeRestful, _ time.Duration) error {
	return errors.New("failed to renew session in tests")
}

func TestUnitTokenAccessorDoesNotRenewStaleToken(t *testing.T) {
	accessor := getSimpleTokenAccessor()
	oldToken := "test"
	accessor.SetTokens(oldToken, "master", 123)

	renewSessionCalled := false
	renewSessionDummy := func(_ context.Context, sr *snowflakeRestful, _ time.Duration) error {
		// should not have gotten to actual renewal
		renewSessionCalled = true
		return nil
	}

	sr := &snowflakeRestful{
		FuncRenewSession: renewSessionDummy,
		TokenAccessor:    accessor,
	}

	// try to intentionally renew with stale token
	assertNilE(t, sr.renewExpiredSessionToken(context.Background(), time.Hour, "stale-token"))

	if renewSessionCalled {
		t.Fatal("FuncRenewSession should not have been called")
	}

	// set the current token to empty, should still call renew even if stale token is passed in
	accessor.SetTokens("", "master", 123)
	assertNilE(t, sr.renewExpiredSessionToken(context.Background(), time.Hour, "stale-token"))

	if !renewSessionCalled {
		t.Fatal("FuncRenewSession should have been called because current token is empty")
	}
}

type wrappedAccessor struct {
	ta              TokenAccessor
	lockCallCount   int32
	unlockCallCount int32
}

func (wa *wrappedAccessor) Lock() error {
	atomic.AddInt32(&wa.lockCallCount, 1)
	err := wa.ta.Lock()
	return err
}

func (wa *wrappedAccessor) Unlock() {
	atomic.AddInt32(&wa.unlockCallCount, 1)
	wa.ta.Unlock()
}

func (wa *wrappedAccessor) GetTokens() (token string, masterToken string, sessionID int64) {
	return wa.ta.GetTokens()
}

func (wa *wrappedAccessor) SetTokens(token string, masterToken string, sessionID int64) {
	wa.ta.SetTokens(token, masterToken, sessionID)
}

func TestUnitTokenAccessorRenewBlocked(t *testing.T) {
	accessor := wrappedAccessor{
		ta: getSimpleTokenAccessor(),
	}
	oldToken := "test"
	accessor.SetTokens(oldToken, "master", 123)

	renewSessionCalled := false
	renewSessionDummy := func(_ context.Context, sr *snowflakeRestful, _ time.Duration) error {
		renewSessionCalled = true
		return nil
	}

	sr := &snowflakeRestful{
		FuncRenewSession: renewSessionDummy,
		TokenAccessor:    &accessor,
	}

	// intentionally lock the accessor first
	assertNilE(t, accessor.Lock())

	// try to intentionally renew with stale token
	var renewalStart sync.WaitGroup
	var renewalDone sync.WaitGroup
	renewalStart.Add(1)
	renewalDone.Add(1)
	go func() {
		renewalStart.Done()
		assertNilE(t, sr.renewExpiredSessionToken(context.Background(), time.Hour, oldToken))
		renewalDone.Done()
	}()

	// wait for renewal to start and get blocked on lock
	renewalStart.Wait()
	// should be blocked and not be able to call renew session
	if renewSessionCalled {
		t.Fail()
	}

	// rotate the token again so that the session token is considered stale
	accessor.SetTokens("new-token", "m", 321)

	// unlock so that renew can happen
	accessor.Unlock()
	renewalDone.Wait()

	// renewal should be done but token should still not
	// have been renewed since we intentionally swapped token while locked
	if renewSessionCalled {
		t.Fail()
	}

	// wait for accessor defer unlock
	assertNilE(t, accessor.Lock())
	if accessor.lockCallCount != 3 {
		t.Fatalf("Expected Lock() to be called thrice, but got %v", accessor.lockCallCount)
	}
	if accessor.unlockCallCount != 2 {
		t.Fatalf("Expected Unlock() to be called twice, but got %v", accessor.unlockCallCount)
	}
}

func TestUnitTokenAccessorRenewSessionContention(t *testing.T) {
	accessor := getSimpleTokenAccessor()
	oldToken := "test"
	accessor.SetTokens(oldToken, "master", 123)
	var counter int32 = 0

	expectedToken := "new token"
	expectedMaster := "new master"
	expectedSession := int64(321)

	renewSessionDummy := func(_ context.Context, sr *snowflakeRestful, _ time.Duration) error {
		accessor.SetTokens(expectedToken, expectedMaster, expectedSession)
		atomic.AddInt32(&counter, 1)
		return nil
	}

	sr := &snowflakeRestful{
		FuncRenewSession: renewSessionDummy,
		TokenAccessor:    accessor,
	}

	var renewalsStart sync.WaitGroup
	var renewalsDone sync.WaitGroup
	var renewalError error
	numRoutines := 50
	for i := 0; i < numRoutines; i++ {
		renewalsDone.Add(1)
		renewalsStart.Add(1)
		go func() {
			// wait for all goroutines to have been created before proceeding to race against each other
			renewalsStart.Wait()
			err := sr.renewExpiredSessionToken(context.Background(), time.Hour, oldToken)
			if err != nil {
				renewalError = err
			}
			renewalsDone.Done()
		}()
	}

	// unlock all of the waiting goroutines simultaneously
	renewalsStart.Add(-numRoutines)

	// wait for all competing goroutines to finish calling renew expired session token
	renewalsDone.Wait()

	if renewalError != nil {
		t.Fatalf("failed to renew session, error %v", renewalError)
	}
	newToken, newMaster, newSession := accessor.GetTokens()
	if newToken != expectedToken {
		t.Fatalf("token %v does not match expected %v", newToken, expectedToken)
	}
	if newMaster != expectedMaster {
		t.Fatalf("master token %v does not match expected %v", newMaster, expectedMaster)
	}
	if newSession != expectedSession {
		t.Fatalf("session %v does not match expected %v", newSession, expectedSession)
	}
	// only the first renewal will go through and FuncRenewSession should be called exactly once
	if counter != 1 {
		t.Fatalf("renew expired session was called more than once: %v", counter)
	}
}

func TestUnitPostQueryHelperUsesToken(t *testing.T) {
	accessor := getSimpleTokenAccessor()
	token := "token123"
	accessor.SetTokens(token, "", 0)

	var err error
	postQueryTest := func(_ context.Context, _ *snowflakeRestful, _ *url.Values, headers map[string]string, _ []byte, _ time.Duration, _ UUID, _ *Config) (*execResponse, error) {
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
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, NewUUID(), &Config{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestUnitPostQueryHelperRenewSession(t *testing.T) {
	var err error
	origRequestID := NewUUID()
	postQueryTest := func(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration, requestID UUID, _ *Config) (*execResponse, error) {
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
	oldToken, oldMasterToken, oldSessionID := "oldtoken", "oldmaster", int64(100)
	newToken, newMasterToken, newSessionID := "newtoken", "newmaster", int64(200)
	postTestSuccessWithNewTokens := func(_ context.Context, _ *snowflakeRestful, _ *url.URL, headers map[string]string, _ []byte, _ time.Duration, _ currentTimeProvider, _ *Config) (*http.Response, error) {
		if headers[headerAuthorizationKey] != fmt.Sprintf(headerSnowflakeToken, oldMasterToken) {
			t.Fatalf("authorization key doesn't match, %v vs %v", headers[headerAuthorizationKey], fmt.Sprintf(headerSnowflakeToken, oldMasterToken))
		}
		tr := &renewSessionResponse{
			Data: renewSessionResponseMain{
				SessionToken: newToken,
				MasterToken:  newMasterToken,
				SessionID:    newSessionID,
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
	accessor.SetTokens(oldToken, oldMasterToken, oldSessionID)
	sr.FuncPost = postTestSuccessWithNewTokens
	err = renewRestfulSession(context.Background(), sr, time.Second)
	if err != nil {
		t.Fatal("should not have failed to run post request after the renewal")
	}
	token, masterToken, sessionID := accessor.GetTokens()
	if token != newToken {
		t.Fatalf("unexpected new token %v", token)
	}
	if masterToken != newMasterToken {
		t.Fatalf("unexpected new master token %v", masterToken)
	}
	if sessionID != newSessionID {
		t.Fatalf("unexpected new session id %v", sessionID)
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
		FuncPost:      postTestAfterRenew,
		TokenAccessor: getSimpleTokenAccessor(),
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

func TestCancelRetry(t *testing.T) {
	sr := &snowflakeRestful{
		TokenAccessor:   getSimpleTokenAccessor(),
		FuncPost:        postTestQueryNotExecuting,
		FuncCancelQuery: cancelTestRetry,
	}
	ctx := context.Background()
	err := cancelQuery(ctx, sr, getOrGenerateRequestIDFromContext(ctx), time.Second)
	if err != nil {
		t.Fatal(err)
	}
}
