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
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

const (
	serviceNameStub   = "SV"
	serviceNameAppend = "a"
)

// postQueryMock generates a response based on the X-Snowflake-Service header,
// to generate a response with the SERVICE_NAME field appending a character at
// the end of the header. This way it could test both the send and receive logic
func postQueryMock(_ context.Context, _ *snowflakeRestful, _ *url.Values,
	headers map[string]string, _ []byte, _ time.Duration, _ uuid.UUID,
	_ *Config) (*execResponse, error) {
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
	postQueryMock := func(_ context.Context, _ *snowflakeRestful,
		_ *url.Values, _ map[string]string, _ []byte, _ time.Duration,
		requestID uuid.UUID, _ *Config) (*execResponse, error) {
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
	if _, err := sc.exec(ctx, "", false /* noResult */, false, /* isInternal */
		false /* describeOnly */, nil); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestGetQueryResultUsesTokenFromTokenAccessor(t *testing.T) {
	ta := getSimpleTokenAccessor()
	token := "snowflake-test-token"
	ta.SetTokens(token, "", 1)
	funcGetMock := func(_ context.Context, _ *snowflakeRestful, _ *url.URL,
		headers map[string]string, _ time.Duration) (*http.Response, error) {
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
	if _, err := sc.getQueryResultResp(context.Background(), ""); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestExecWithSpecificRequestID(t *testing.T) {
	origRequestID := uuid.New()
	ctx := WithRequestID(context.Background(), origRequestID)
	postQueryMock := func(_ context.Context, _ *snowflakeRestful,
		_ *url.Values, _ map[string]string, _ []byte, _ time.Duration,
		requestID uuid.UUID, _ *Config) (*execResponse, error) {
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
	if _, err := sc.exec(ctx, "", false /* noResult */, false, /* isInternal */
		false /* describeOnly */, nil); err != nil {
		t.Fatalf("err: %v", err)
	}
}

// TestServiceName tests two things:
// 1. request header contains X-Snowflake-Service if the cfg parameters
// contains SERVICE_NAME
// 2. SERVICE_NAME is updated by response payload
// Uses interactive postQueryMock that generates a response based on header
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
		sc.exec(context.TODO(), "", false, /* noResult */
			false /* isInternal */, false /* describeOnly */, nil)
		if actualServiceName, ok := sc.cfg.Params[serviceName]; ok {
			if *actualServiceName != expectServiceName {
				t.Errorf("service name mis-match. expected %v, actual %v",
					expectServiceName, actualServiceName)
			}
		} else {
			t.Error("No service name in the response")
		}
		expectServiceName += serviceNameAppend
	}
}

var closedSessionCount = 0

var testTelemetry = &snowflakeTelemetry{
	mutex: &sync.Mutex{},
}

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
		cfg:       &Config{Params: map[string]*string{}},
		rest:      sr,
		telemetry: testTelemetry,
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
		cfg:       &Config{Params: map[string]*string{}},
		rest:      sr,
		telemetry: testTelemetry,
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
	fetchResultByQueryID(t, returnQueryIsErrStatus, &SnowflakeError{
		Number: ErrQueryReportedError})
}

func customGetQuery(ctx context.Context, rest *snowflakeRestful, url *url.URL,
	vals map[string]string, _ time.Duration, jsonStr string) (
	*http.Response, error) {
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
	jsonStr := `{"data" : { "queries" : [{"status" : "RUNNING", "state" :
		"FILE_SET_INITIALIZATION", "errorCode" : 0, "errorMessage" : null}] },
		"code" : null, "message" : null, "success" : true }`
	return customGetQuery(ctx, rest, fullURL, vals, duration, jsonStr)
}

func returnQueryIsErrStatus(ctx context.Context, rest *snowflakeRestful, fullURL *url.URL,
	vals map[string]string, duration time.Duration) (*http.Response, error) {
	jsonStr := `{"data" : { "queries" : [{"status" : "FAILED_WITH_ERROR",
		"errorCode" : 0, "errorMessage" : ""}] }, "code" : null, "message" :
		null, "success" : true }`
	return customGetQuery(ctx, rest, fullURL, vals, duration, jsonStr)
}

// this function is going to: 1, create a table, 2, query on this table,
// 3, fetch result of query in step 2, mock running status and error status
// of that query.
func fetchResultByQueryID(
	t *testing.T,
	customGet FuncGetType,
	expectedFetchErr *SnowflakeError) error {
	config, _ := ParseDSN(dsn)
	ctx := context.Background()
	sc, err := buildSnowflakeConn(ctx, *config)
	if customGet != nil {
		sc.rest.FuncGet = customGet
	}
	if err != nil {
		return err
	}
	if err = authenticateWithConfig(sc); err != nil {
		return err
	}

	if _, err = sc.Exec(`create or replace table ut_conn(c1 number, c2 string)
		as (select seq4() as seq, concat('str',to_varchar(seq)) as str1 from
		table(generator(rowcount => 100)))`, nil); err != nil {
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
		if err = rows2.Next(dest); err != nil {
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

func TestPrivateLink(t *testing.T) {
	if _, err := buildSnowflakeConn(context.Background(), Config{
		Account:  "testaccount",
		User:     "testuser",
		Password: "testpassword",
		Host:     "testaccount.us-east-1.privatelink.snowflakecomputing.com",
	}); err != nil {
		t.Error(err)
	}
	ocspURL := os.Getenv(cacheServerURLEnv)
	expectedURL := "http://ocsp.testaccount.us-east-1.privatelink.snowflakecomputing.com/ocsp_response_cache.json"
	if ocspURL != expectedURL {
		t.Errorf("expected: %v, got: %v", expectedURL, ocspURL)
	}
	retryURL := os.Getenv(ocspRetryURLEnv)
	expectedURL = "http://ocsp.testaccount.us-east-1.privatelink.snowflakecomputing.com/retry/%v/%v"
	if retryURL != expectedURL {
		t.Errorf("expected: %v, got: %v", expectedURL, retryURL)
	}
}

func TestGetQueryStatus(t *testing.T) {
	config, _ := ParseDSN(dsn)
	ctx := context.Background()
	sc, err := buildSnowflakeConn(ctx, *config)
	if err != nil {
		t.Error(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Error(err)
	}

	if _, err = sc.Exec(`create or replace table ut_conn(c1 number, c2 string)
		as (select seq4() as seq, concat('str',to_varchar(seq)) as str1 from
		table(generator(rowcount => 100)))`, nil); err != nil {
		t.Error(err)
	}

	rows, err := sc.QueryContext(ctx, "select min(c1) as ms, sum(c1) from ut_conn group by (c1 % 10) order by ms", nil)
	if err != nil {
		t.Error(err)
	}
	qid := rows.(SnowflakeResult).GetQueryID()

	// use conn as type holder for SnowflakeConnection placeholder
	var conn interface{} = sc
	qStatus, err := conn.(SnowflakeConnection).GetQueryStatus(ctx, qid)
	if err != nil {
		t.Error(err)
	}

	if qStatus.ErrorCode != 0 || qStatus.ScanBytes != 1536 || qStatus.ProducedRows != 10 {
		t.Errorf("expected no error. got: %v, scan bytes: %v, produced rows: %v",
			qStatus.ErrorCode, qStatus.ScanBytes, qStatus.ProducedRows)
	}
}
