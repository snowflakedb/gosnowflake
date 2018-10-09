// Copyright (c) 2017-2018 Snowflake Computing Inc. All right reserved.
package gosnowflake

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func postTestError(_ context.Context, _ *snowflakeRestful, _ string, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, errors.New("failed to run post method")
}

func postTestSuccessButInvalidJSON(_ context.Context, _ *snowflakeRestful, _ string, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestAppBadGatewayError(_ context.Context, _ *snowflakeRestful, _ string, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestAppForbiddenError(_ context.Context, _ *snowflakeRestful, _ string, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusForbidden,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestAppUnexpectedError(_ context.Context, _ *snowflakeRestful, _ string, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusInsufficientStorage,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestRenew(_ context.Context, _ *snowflakeRestful, _ string, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	dd := &execResponseData{}
	er := &execResponse{
		Data:    *dd,
		Message: "",
		Code:    sessionExpiredCode,
		Success: true,
	}

	ba, err := json.Marshal(er)
	glog.V(2).Infof("encoded JSON: %v", ba)
	if err != nil {
		panic(err)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: ba},
	}, nil
}

func postTestAfterRenew(_ context.Context, _ *snowflakeRestful, _ string, _ map[string]string, _ []byte, _ time.Duration, _ bool) (*http.Response, error) {
	dd := &execResponseData{}
	er := &execResponse{
		Data:    *dd,
		Message: "",
		Code:    "",
		Success: true,
	}

	ba, err := json.Marshal(er)
	glog.V(2).Infof("encoded JSON: %v", ba)
	if err != nil {
		panic(err)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: ba},
	}, nil
}

func postQueryTest(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration) (*execResponse, error) {
	dd := &execResponseData{}
	return &execResponse{
		Data:    *dd,
		Message: "",
		Code:    "0",
		Success: true,
	}, nil
}

func TestUnitPostQueryHelperError(t *testing.T) {
	sr := &snowflakeRestful{
		Token:    "token",
		FuncPost: postTestError,
	}
	var err error
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, "abcdefg")
	if err == nil {
		t.Fatalf("should have failed to post")
	}
	sr.FuncPost = postTestAppBadGatewayError
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, "abcdefg")
	if err == nil {
		t.Fatalf("should have failed to post")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, "abcdefg")
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

func TestUnitPostQueryHelperRenewSession(t *testing.T) {
	sr := &snowflakeRestful{
		Token:            "token",
		FuncPost:         postTestRenew,
		FuncPostQuery:    postQueryTest,
		FuncRenewSession: renewSessionTest,
	}
	var err error
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, "abcdefg")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sr.FuncRenewSession = renewSessionTestError
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, "abcdefg")
	if err == nil {
		t.Fatal("should have failed to renew session")
	}
}

func TestUnitRenewRestfulSession(t *testing.T) {
	sr := &snowflakeRestful{
		MasterToken: "mtoken",
		Token:       "token",
		FuncPost:    postTestAfterRenew,
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
}

func TestUnitCloseSession(t *testing.T) {
	sr := &snowflakeRestful{
		FuncPost: postTestAfterRenew,
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
	err := cancelQuery(context.Background(), sr, "abcdefg", time.Second)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sr.FuncPost = postTestError
	err = cancelQuery(context.Background(), sr, "abcdefg", time.Second)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
	sr.FuncPost = postTestAppBadGatewayError
	err = cancelQuery(context.Background(), sr, "abcdefg", time.Second)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
	sr.FuncPost = postTestSuccessButInvalidJSON
	err = cancelQuery(context.Background(), sr, "abcdefg", time.Second)
	if err == nil {
		t.Fatal("should have failed to close session")
	}
}
