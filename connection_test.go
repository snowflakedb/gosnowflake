// Copyright (c) 2019-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	serviceNameStub   = "SV"
	serviceNameAppend = "a"
)

func TestInvalidConnection(t *testing.T) {
	db := openDB(t)
	if err := db.Close(); err != nil {
		t.Error("should not cause error in Close")
	}
	if err := db.Close(); err != nil {
		t.Error("should not cause error in the second call of Close")
	}
	if _, err := db.Exec("CREATE TABLE OR REPLACE test0(c1 int)"); err == nil {
		t.Error("should fail to run Exec")
	}
	if _, err := db.Query("SELECT CURRENT_TIMESTAMP()"); err == nil {
		t.Error("should fail to run Query")
	}
	if _, err := db.Begin(); err == nil {
		t.Error("should fail to run Begin")
	}
}

// postQueryMock generates a response based on the X-Snowflake-Service header,
// to generate a response with the SERVICE_NAME field appending a character at
// the end of the header. This way it could test both the send and receive logic
func postQueryMock(_ context.Context, _ *snowflakeRestful, _ *url.Values,
	headers map[string]string, _ []byte, _ time.Duration, _ UUID,
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
	ctx := WithRequestID(context.Background(), nilUUID)
	postQueryMock := func(_ context.Context, _ *snowflakeRestful,
		_ *url.Values, _ map[string]string, _ []byte, _ time.Duration,
		requestID UUID, _ *Config) (*execResponse, error) {
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
	origRequestID := NewUUID()
	ctx := WithRequestID(context.Background(), origRequestID)
	postQueryMock := func(_ context.Context, _ *snowflakeRestful,
		_ *url.Values, _ map[string]string, _ []byte, _ time.Duration,
		requestID UUID, _ *Config) (*execResponse, error) {
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
			Body:       io.NopCloser(strings.NewReader(jsonStr)),
		}, nil
	}
	return getRestful(ctx, rest, url, vals, rest.RequestTimeout)
}

func returnQueryIsRunningStatus(ctx context.Context, rest *snowflakeRestful, fullURL *url.URL,
	vals map[string]string, duration time.Duration) (*http.Response, error) {
	jsonStr := `{"data" : { "queries" : [{"status" : "RUNNING", "state" :
		"FILE_SET_INITIALIZATION", "errorCode" : "", "errorMessage" : null}] },
		"code" : null, "message" : null, "success" : true }`
	return customGetQuery(ctx, rest, fullURL, vals, duration, jsonStr)
}

func returnQueryIsErrStatus(ctx context.Context, rest *snowflakeRestful, fullURL *url.URL,
	vals map[string]string, duration time.Duration) (*http.Response, error) {
	jsonStr := `{"data" : { "queries" : [{"status" : "FAILED_WITH_ERROR",
		"errorCode" : "", "errorMessage" : ""}] }, "code" : null, "message" :
		null, "success" : true }`
	return customGetQuery(ctx, rest, fullURL, vals, duration, jsonStr)
}

// this function is going to: 1, create a table, 2, query on this table,
// 3, fetch result of query in step 2, mock running status and error status
// of that query.
func fetchResultByQueryID(
	t *testing.T,
	customGet funcGetType,
	expectedFetchErr *SnowflakeError) error {
	config, err := ParseDSN(dsn)
	if err != nil {
		return err
	}
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
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
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
		t.Errorf("failed to get query status err = %s", err.Error())
		return
	}
	if qStatus == nil {
		t.Error("there was no query status returned")
		return
	}

	if qStatus.ErrorCode != "" || qStatus.ScanBytes != 2048 || qStatus.ProducedRows != 10 {
		t.Errorf("expected no error. got: %v, scan bytes: %v, produced rows: %v",
			qStatus.ErrorCode, qStatus.ScanBytes, qStatus.ProducedRows)
		return
	}
}

func TestGetInvalidQueryStatus(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	sc, err := buildSnowflakeConn(ctx, *config)
	if err != nil {
		t.Error(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Error(err)
	}

	sc.rest.RequestTimeout = 1 * time.Second

	qStatus, err := sc.checkQueryStatus(ctx, "1234")
	if err == nil || qStatus != nil {
		t.Error("expected an error")
	}
}

func TestExecWithServerSideError(t *testing.T) {
	postQueryMock := func(_ context.Context, _ *snowflakeRestful,
		_ *url.Values, _ map[string]string, _ []byte, _ time.Duration,
		requestID UUID, _ *Config) (*execResponse, error) {
		dd := &execResponseData{}
		return &execResponse{
			Data:    *dd,
			Message: "",
			Code:    "",
			Success: false,
		}, nil
	}

	sr := &snowflakeRestful{
		FuncPostQuery: postQueryMock,
	}
	sc := &snowflakeConn{
		cfg:       &Config{Params: map[string]*string{}},
		rest:      sr,
		telemetry: testTelemetry,
	}
	_, err := sc.exec(context.Background(), "", false, /* noResult */
		false /* isInternal */, false /* describeOnly */, nil)
	if err == nil {
		t.Error("expected a server side error")
	}
	sfe := err.(*SnowflakeError)
	if sfe.Number != -1 || sfe.SQLState != "-1" || sfe.QueryID != "-1" {
		t.Errorf("incorrect snowflake error. expected: %v, got: %v", ErrUnknownError, *sfe)
	}
	if !strings.Contains(sfe.Message, "an unknown server side error occurred") {
		t.Errorf("incorrect message. expected: %v, got: %v", ErrUnknownError.Message, sfe.Message)
	}
}

func TestConcurrentReadOnParams(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal("Failed to parse dsn")
	}
	connector := NewConnector(SnowflakeDriver{}, *config)
	db := sql.OpenDB(connector)
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for c := 0; c < 10; c++ {
				stmt, err := db.PrepareContext(context.Background(), "SELECT * FROM information_schema.columns WHERE table_schema = ?")
				if err != nil {
					t.Error(err)
				}
				rows, err := stmt.Query("INFORMATION_SCHEMA")
				if err != nil {
					t.Error(err)
				}
				if rows == nil {
					continue
				}
				_ = rows.Close()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	defer db.Close()
}
