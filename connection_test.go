// Copyright (c) 2019-2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
)

const serviceNameStub = "SV"
const serviceNameAppend = "a"

// postQueryMock would generate a response based on the X-Snowflake-Service header, to generate a response
// with the SERVICE_NAME field appending a character at the end of the header
// This way it could test both the send and receive logic
func postQueryMock(_ context.Context, _ *snowflakeRestful, _ *url.Values, headers map[string]string, _ []byte, _ time.Duration, _ uuid.UUID, _ *Config) (*execResponse, error) {
	var serviceName string
	if serviceHeader, ok := headers["X-Snowflake-Service"]; ok {
		serviceName = serviceHeader + serviceNameAppend
	} else {
		serviceName = serviceNameStub
	}

	dd := &execResponseData{
		Parameters: []nameValueParameter{{"SERVICE_NAME", serviceName}},
	}
	return &execResponse{
		Data:    *dd,
		Message: "",
		Code:    "0",
		Success: true,
	}, nil
}

func TestExecWithEmptyRequestID(t *testing.T) {
	ctx := WithRequestID(context.Background(), uuid.Nil)
	postQueryMock := func(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration, requestID uuid.UUID, _ *Config) (*execResponse, error) {
		// ensure the same requestID from context is used
		if len(requestID) == 0 {
			t.Fatal("requestID is empty")
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
		FuncPostQuery: postQueryMock,
	}

	sc := &snowflakeConn{
		cfg:  &Config{Params: map[string]*string{}},
		rest: sr,
	}
	_, err := sc.exec(ctx, "", false /* noResult */, false /* isInternal */, false /* describeOnly */, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestGetQueryResultUsesTokenFromTokenAccessor(t *testing.T) {
	ta := getSimpleTokenAccessor()
	token := "snowflake-test-token"
	ta.SetTokens(token, "", 1)
	funcGetMock := func(_ context.Context, _ *snowflakeRestful, _ *url.URL, headers map[string]string, _ time.Duration) (*http.Response, error) {
		if headers[headerAuthorizationKey] != fmt.Sprintf(headerSnowflakeToken, token) {
			t.Fatalf("header authorization key is not correct: %v", headers[headerAuthorizationKey])
		}
		dd := &execResponseData{}
		er := &execResponse{
			Data:    *dd,
			Message: "",
			Code:    sessionExpiredCode,
			Success: true,
		}
		ba, err := json.Marshal(er)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       &fakeResponseBody{body: ba},
		}, nil
	}
	sr := &snowflakeRestful{
		FuncGet:       funcGetMock,
		TokenAccessor: ta,
	}
	sc := &snowflakeConn{
		cfg:  &Config{Params: map[string]*string{}},
		rest: sr,
	}
	_, err := sc.getQueryResult(context.Background(), "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestExecWithSpecificRequestID(t *testing.T) {
	origRequestID := uuid.New()
	ctx := WithRequestID(context.Background(), origRequestID)
	postQueryMock := func(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration, requestID uuid.UUID, _ *Config) (*execResponse, error) {
		// ensure the same requestID from context is used
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
		FuncPostQuery: postQueryMock,
	}

	sc := &snowflakeConn{
		cfg:  &Config{Params: map[string]*string{}},
		rest: sr,
	}
	_, err := sc.exec(ctx, "", false /* noResult */, false /* isInternal */, false /* describeOnly */, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

// TestServiceName tests two things:
// 1. request header would contain X-Snowflake-Service if the cfg parameters contains SERVICE_NAME
// 2. SERVICE_NAME would be update by response payload
// It is achieved through an interactive postQueryMock that would generate response based on header
func TestServiceName(t *testing.T) {
	sr := &snowflakeRestful{
		FuncPostQuery: postQueryMock,
	}

	sc := &snowflakeConn{
		cfg:  &Config{Params: map[string]*string{}},
		rest: sr,
	}

	expectServiceName := serviceNameStub
	for i := 0; i < 5; i++ {
		sc.exec(context.TODO(), "", false /* noResult */, false /* isInternal */, false /* describeOnly */, nil)
		if actualServiceName, ok := sc.cfg.Params[serviceName]; ok {
			if *actualServiceName != expectServiceName {
				t.Errorf("service name mis-match. expected %v, actual %v", expectServiceName, actualServiceName)
			}
		} else {
			t.Error("No service name in the response")
		}

		expectServiceName += serviceNameAppend
	}
}

func closeSessionMock(_ context.Context, _ *snowflakeRestful, _ time.Duration) error {
	return &SnowflakeError{
		Number: ErrSessionGone,
	}
}

func TestCloseIgnoreSessionGone(t *testing.T) {
	sr := &snowflakeRestful{
		FuncCloseSession: closeSessionMock,
	}
	sc := &snowflakeConn{
		cfg:  &Config{Params: map[string]*string{}},
		rest: sr,
	}

	if sc.Close() != nil {
		t.Error("Close should let go session gone error")
	}
}
