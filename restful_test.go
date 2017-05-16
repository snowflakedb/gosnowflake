// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
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

	"github.com/golang/glog"
)

func postTestError(_ context.Context, _ *snowflakeRestful, _ string, _ map[string]string, _ []byte, _ time.Duration) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, errors.New("failed to run post method")
}

func postTestAppError(_ context.Context, _ *snowflakeRestful, _ string, _ map[string]string, _ []byte, _ time.Duration) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}, nil
}

func postTestRenew(_ context.Context, _ *snowflakeRestful, _ string, _ map[string]string, _ []byte, _ time.Duration) (*http.Response, error) {
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

func postQueryTest(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration) (*execResponse, error) {
	dd := &execResponseData{}
	return &execResponse{
		Data:    *dd,
		Message: "",
		Code:    "0",
		Success: true,
	}, nil
}

func postQueryHelperTestError(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration, _ string) (*execResponse, error) {
	dd := &execResponseData{}
	return &execResponse{
		Data:    *dd,
		Message: "",
		Code:    "0",
		Success: false,
	}, fmt.Errorf("failed to run postQueryHelper")
}

func postQueryHelperTest(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration, _ string) (*execResponse, error) {
	dd := &execResponseData{}
	return &execResponse{
		Data:    *dd,
		Message: "",
		Code:    "0",
		Success: true,
	}, nil
}

func TestPostQueryHelperError(t *testing.T) {
	sr := &snowflakeRestful{
		Token:    "token",
		FuncPost: postTestError,
		//FuncPostQuery: postQueryTest,
		//FuncRenewSession: renewSessionTest,
	}
	var err error
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, "abcdefg")
	if err == nil {
		t.Fatalf("should have failed to post")
	}
	sr.FuncPost = postTestAppError
	_, err = postRestfulQueryHelper(context.Background(), sr, &url.Values{}, make(map[string]string), []byte{0x12, 0x34}, 0, "abcdefg")
	if err == nil {
		t.Fatalf("should have failed to post")
	}
}

func renewSessionTest(_ context.Context, _ *snowflakeRestful) error {
	return nil
}

func renewSessionTestError(_ context.Context, _ *snowflakeRestful) error {
	return errors.New("failed to renew session in tests")
}

func TestPostQueryHelperRenewSession(t *testing.T) {
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
