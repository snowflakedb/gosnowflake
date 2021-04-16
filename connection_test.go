// Copyright (c) 2019-2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
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
	if serviceHeader, ok := headers[httpHeaderServiceName]; ok {
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
	_, err := sc.getQueryResultResp(context.Background(), "")
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

var closedSessionCount = 0

func closeSessionMock(_ context.Context, _ *snowflakeRestful, _ time.Duration) error {
	closedSessionCount++
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

func TestClientSessionPersist(t *testing.T) {
	sr := &snowflakeRestful{
		FuncCloseSession: closeSessionMock,
	}
	sc := &snowflakeConn{
		cfg:  &Config{Params: map[string]*string{}},
		rest: sr,
	}
	sc.cfg.KeepSessionAlive = true
	count := closedSessionCount
	if sc.Close() != nil {
		t.Error("Connection close should not return error")
	}
	if count != closedSessionCount {
		t.Fatal("close session was called")
	}
}

func TestFetchResultByQueryID(t *testing.T) {
	fetchResultByQueryID(t, nil, nil)
}

func TestFetchRunningQueryByID(t *testing.T) {
	fetchResultByQueryID(t, returnQueryIsRunningStatus, nil)
}

func TestFetchErrorQueryByID(t *testing.T) {
	err := &SnowflakeError{
		Number: ErrQueryReportedError}
	fetchResultByQueryID(t, returnQueryIsErrStatus, err)
}

func customGetQuery(ctx context.Context, rest *snowflakeRestful, url *url.URL,
	vals map[string]string, _ time.Duration, jsonStr string) (*http.Response, error) {
	if strings.Contains(url.Path, "/monitoring/queries/") {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(strings.NewReader(jsonStr)),
		}, nil
	}
	return getRestful(ctx, rest, url, vals, rest.RequestTimeout)
}

func returnQueryIsRunningStatus(ctx context.Context, rest *snowflakeRestful, fullURL *url.URL,
	vals map[string]string, duration time.Duration) (*http.Response, error) {
	var jsonStr = `{"data" : { "queries" : [{"status" : "RUNNING", "state" : "FILE_SET_INITIALIZATION",
        "errorCode" : 0, "errorMessage" : null}] }, "code" : null, "message" : null, "success" : true }`
	return customGetQuery(ctx, rest, fullURL, vals, duration, jsonStr)
}

func returnQueryIsErrStatus(ctx context.Context, rest *snowflakeRestful, fullURL *url.URL,
	vals map[string]string, duration time.Duration) (*http.Response, error) {
	var jsonStr = `{"data" : { "queries" : [{"status" : "FAILED_WITH_ERROR", "errorCode" : 0, "errorMessage" : ""}] },
       "code" : null, "message" : null, "success" : true }`
	return customGetQuery(ctx, rest, fullURL, vals, duration, jsonStr)
}

// this function is going to: 1, create a table, 2, query on this table,
//      3, fetch result of query in step 2, mock running status and error status of that query.
func fetchResultByQueryID(t *testing.T, customget FuncGetType, expectedFetchErr *SnowflakeError) error {
	config, _ := ParseDSN(dsn)
	ctx := context.Background()
	sc, err := buildSnowflakeConn(ctx, *config)
	if customget != nil {
		sc.rest.FuncGet = customget
	}
	if err != nil {
		return err
	}
	err = authenticateWithConfig(sc)
	if err != nil {
		return err
	}

	_, err = sc.Exec("create or replace table ut_conn(c1 number, c2 string)"+
		" as (select seq4() as seq, concat('str',to_varchar(seq)) as str1 from table(generator(rowcount => 100)))", nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	rows1, err := sc.QueryContext(ctx, "select min(c1) as ms, sum(c1) from ut_conn group by (c1 % 10) order by ms", nil)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	qid := rows1.(SnowflakeResult).GetQueryID()
	newCtx := WithFetchResultByID(ctx, qid)

	rows2, err := sc.QueryContext(newCtx, "", nil)
	if err != nil {
		if expectedFetchErr != nil { // got expected error number
			if expectedFetchErr.Number == err.(*SnowflakeError).Number {
				return nil
			}
		}
		t.Fatalf("Fetch Query Result by ID failed: %v", err)
	}

	dest := make([]driver.Value, 2)
	cnt := 0
	for {
		err = rows2.Next(dest)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				t.Fatalf("unexpected error: %v", err)
			}
		}
		cnt++
	}
	if cnt != 10 {
		t.Fatalf("rowcount is not expected 10: %v", cnt)
	}
	return nil
}
