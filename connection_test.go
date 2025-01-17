// Copyright (c) 2019-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
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
	if _, err := db.ExecContext(context.Background(), "CREATE TABLE OR REPLACE test0(c1 int)"); err == nil {
		t.Error("should fail to run Exec")
	}
	if _, err := db.QueryContext(context.Background(), "SELECT CURRENT_TIMESTAMP()"); err == nil {
		t.Error("should fail to run Query")
	}
	if _, err := db.BeginTx(context.Background(), nil); err == nil {
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
		cfg:               &Config{Params: map[string]*string{}},
		rest:              sr,
		queryContextCache: (&queryContextCache{}).init(),
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
			Code:    "0",
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
		cfg:                 &Config{Params: map[string]*string{}},
		rest:                sr,
		currentTimeProvider: defaultTimeProvider,
	}
	if _, err := sc.getQueryResultResp(context.Background(), ""); err != nil {
		t.Fatalf("err: %v", err)
	}
}

func TestGetQueryResultTokenExpiry(t *testing.T) {
	ta := getSimpleTokenAccessor()
	token := "snowflake-test-token"
	ta.SetTokens(token, "", 1)
	funcGetMock := func(_ context.Context, _ *snowflakeRestful, _ *url.URL,
		headers map[string]string, _ time.Duration) (*http.Response, error) {
		respData := execResponseData{}
		er := &execResponse{
			Data:    respData,
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

	expectedToken := "new token"
	expectedMaster := "new master"
	expectedSession := int64(321)

	renewSessionDummy := func(_ context.Context, sr *snowflakeRestful, _ time.Duration) error {
		ta.SetTokens(expectedToken, expectedMaster, expectedSession)
		return nil
	}

	sr := &snowflakeRestful{
		FuncGet:          funcGetMock,
		FuncRenewSession: renewSessionDummy,
		TokenAccessor:    ta,
	}
	sc := &snowflakeConn{
		cfg:                 &Config{Params: map[string]*string{}},
		rest:                sr,
		currentTimeProvider: defaultTimeProvider,
	}
	_, err := sc.getQueryResultResp(context.Background(), "")
	assertNilF(t, err, fmt.Sprintf("err: %v", err))

	updatedToken, updatedMaster, updatedSession := ta.GetTokens()
	assertEqualF(t, updatedToken, expectedToken)
	assertEqualF(t, updatedMaster, expectedMaster)
	assertEqualF(t, updatedSession, expectedSession)
}

func TestGetQueryResultTokenNotSet(t *testing.T) {
	ta := getSimpleTokenAccessor()
	funcGetMock := func(_ context.Context, _ *snowflakeRestful, _ *url.URL,
		headers map[string]string, _ time.Duration) (*http.Response, error) {
		respData := execResponseData{}
		er := &execResponse{
			Data:    respData,
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

	expectedToken := "new token"
	expectedMaster := "new master"
	expectedSession := int64(321)

	renewSessionDummy := func(_ context.Context, sr *snowflakeRestful, _ time.Duration) error {
		ta.SetTokens(expectedToken, expectedMaster, expectedSession)
		return nil
	}

	sr := &snowflakeRestful{
		FuncGet:          funcGetMock,
		FuncRenewSession: renewSessionDummy,
		TokenAccessor:    ta,
	}
	sc := &snowflakeConn{
		cfg:                 &Config{Params: map[string]*string{}},
		rest:                sr,
		currentTimeProvider: defaultTimeProvider,
	}
	_, err := sc.getQueryResultResp(context.Background(), "")
	assertNilF(t, err, fmt.Sprintf("err: %v", err))

	updatedToken, updatedMaster, updatedSession := ta.GetTokens()
	assertEqualF(t, updatedToken, expectedToken)
	assertEqualF(t, updatedMaster, expectedMaster)
	assertEqualF(t, updatedSession, expectedSession)
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
		cfg:               &Config{Params: map[string]*string{}},
		rest:              sr,
		queryContextCache: (&queryContextCache{}).init(),
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
		cfg:               &Config{Params: map[string]*string{}},
		rest:              sr,
		queryContextCache: (&queryContextCache{}).init(),
	}

	expectServiceName := serviceNameStub
	for i := 0; i < 5; i++ {
		_, err := sc.exec(context.Background(), "", false, /* noResult */
			false /* isInternal */, false /* describeOnly */, nil)
		assertNilF(t, err)
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
		cfg:               &Config{Params: map[string]*string{}},
		rest:              sr,
		telemetry:         testTelemetry,
		queryContextCache: (&queryContextCache{}).init(),
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
	assertNilE(t, fetchResultByQueryID(t, nil, nil))
}

func TestFetchRunningQueryByID(t *testing.T) {
	assertNilE(t, fetchResultByQueryID(t, returnQueryIsRunningStatus, nil))
}

func TestFetchErrorQueryByID(t *testing.T) {
	assertNilE(t, fetchResultByQueryID(t, returnQueryIsErrStatus, &SnowflakeError{
		Number: ErrQueryReportedError}))
}

func TestFetchMalformedJsonQueryByID(t *testing.T) {
	expectedErr := errors.New("invalid character '}' after object key")
	assertNilE(t, fetchResultByQueryID(t, returnQueryMalformedJSON, expectedErr))
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

func returnQueryMalformedJSON(ctx context.Context, rest *snowflakeRestful, fullURL *url.URL,
	vals map[string]string, duration time.Duration) (*http.Response, error) {
	jsonStr := `{"malformedJson"}`
	return customGetQuery(ctx, rest, fullURL, vals, duration, jsonStr)
}

// this function is going to: 1, create a table, 2, query on this table,
// 3, fetch result of query in step 2, mock running status and error status
// of that query.
func fetchResultByQueryID(
	t *testing.T,
	customGet funcGetType,
	expectedFetchErr error) error {
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
							as (select seq4() as seq, concat('str',to_varchar(seq)) as str1 
							from table(generator(rowcount => 100)))`, nil); err != nil {
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
		snowflakeErr, ok := err.(*SnowflakeError)
		if ok && expectedFetchErr != nil { // got expected error number
			if expectedSnowflakeErr, ok := expectedFetchErr.(*SnowflakeError); ok {
				if expectedSnowflakeErr.Number == snowflakeErr.Number {
					return nil
				}
			}
		} else if !ok { // not a SnowflakeError
			if strings.Contains(err.Error(), expectedFetchErr.Error()) {
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

func TestIsPrivateLink(t *testing.T) {
	for _, tc := range []struct {
		host          string
		isPrivatelink bool
	}{
		{"testaccount.us-east-1.snowflakecomputing.com", false},
		{"testaccount-no-privatelink.snowflakecomputing.com", false},
		{"testaccount.us-east-1.privatelink.snowflakecomputing.com", true},
		{"testaccount.cn-region.snowflakecomputing.cn", false},
		{"testaccount.cn-region.privaTELINk.snowflakecomputing.cn", true},
		{"testaccount.some-region.privatelink.snowflakecomputing.mil", true},
		{"testaccount.us-east-1.privatelink.snowflakecOMPUTING.com", true},
		{"snowhouse.snowflakecomputing.xyz", false},
		{"snowhouse.privatelink.snowflakecomputing.xyz", true},
		{"snowhouse.PRIVATELINK.snowflakecomputing.xyz", true},
	} {
		t.Run(tc.host, func(t *testing.T) {
			assertEqualE(t, isPrivateLink(tc.host), tc.isPrivatelink)
		})
	}
}

func TestBuildPrivatelinkConn(t *testing.T) {
	os.Unsetenv(cacheServerURLEnv)
	os.Unsetenv(ocspRetryURLEnv)

	if _, err := buildSnowflakeConn(context.Background(), Config{
		Account:  "testaccount",
		User:     "testuser",
		Password: "testpassword",
		Host:     "testaccount.us-east-1.privatelink.snowflakecomputing.com",
	}); err != nil {
		t.Error(err)
	}
	defer func() {
		os.Unsetenv(cacheServerURLEnv)
		os.Unsetenv(ocspRetryURLEnv)
	}()

	ocspURL := os.Getenv(cacheServerURLEnv)
	assertEqualE(t, ocspURL, "http://ocsp.testaccount.us-east-1.privatelink.snowflakecomputing.com/ocsp_response_cache.json")
	retryURL := os.Getenv(ocspRetryURLEnv)
	assertEqualE(t, retryURL, "http://ocsp.testaccount.us-east-1.privatelink.snowflakecomputing.com/retry/%v/%v")
}

func TestOcspEnvVarsSetup(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		host                string
		cacheURL            string
		privateLinkRetryURL string
	}{
		{
			host:                "testaccount.us-east-1.snowflakecomputing.com",
			cacheURL:            "", // no privatelink, default ocsp cache URL, no need to setup env vars
			privateLinkRetryURL: "",
		},
		{
			host:                "testaccount-no-privatelink.snowflakecomputing.com",
			cacheURL:            "", // no privatelink, default ocsp cache URL, no need to setup env vars
			privateLinkRetryURL: "",
		},
		{
			host:                "testaccount.us-east-1.privatelink.snowflakecomputing.com",
			cacheURL:            "http://ocsp.testaccount.us-east-1.privatelink.snowflakecomputing.com/ocsp_response_cache.json",
			privateLinkRetryURL: "http://ocsp.testaccount.us-east-1.privatelink.snowflakecomputing.com/retry/%v/%v",
		},
		{
			host:                "testaccount.cn-region.snowflakecomputing.cn",
			cacheURL:            "http://ocsp.testaccount.cn-region.snowflakecomputing.cn/ocsp_response_cache.json",
			privateLinkRetryURL: "", // not a privatelink env, no need to setup retry URL
		},
		{
			host:                "testaccount.cn-region.privaTELINk.snowflakecomputing.cn",
			cacheURL:            "http://ocsp.testaccount.cn-region.privatelink.snowflakecomputing.cn/ocsp_response_cache.json",
			privateLinkRetryURL: "http://ocsp.testaccount.cn-region.privatelink.snowflakecomputing.cn/retry/%v/%v",
		},
		{
			host:                "testaccount.some-region.privatelink.snowflakecomputing.mil",
			cacheURL:            "http://ocsp.testaccount.some-region.privatelink.snowflakecomputing.mil/ocsp_response_cache.json",
			privateLinkRetryURL: "http://ocsp.testaccount.some-region.privatelink.snowflakecomputing.mil/retry/%v/%v",
		},
	} {
		t.Run(tc.host, func(t *testing.T) {
			if err := setupOCSPEnvVars(ctx, tc.host); err != nil {
				t.Errorf("error during OCSP env vars setup; %v", err)
			}
			defer func() {
				os.Unsetenv(cacheServerURLEnv)
				os.Unsetenv(ocspRetryURLEnv)
			}()

			cacheURLFromEnv := os.Getenv(cacheServerURLEnv)
			assertEqualE(t, cacheURLFromEnv, tc.cacheURL)
			retryURL := os.Getenv(ocspRetryURLEnv)
			assertEqualE(t, retryURL, tc.privateLinkRetryURL)

		})
	}
}

func TestGetQueryStatus(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		sct.mustExec(`create or replace table ut_conn(c1 number, c2 string)
						as (select seq4() as seq, concat('str',to_varchar(seq)) as str1 
						from table(generator(rowcount => 100)))`,
			nil)

		rows := sct.mustQueryContext(sct.sc.ctx, "select min(c1) as ms, sum(c1) from ut_conn group by (c1 % 10) order by ms", nil)
		qid := rows.(SnowflakeResult).GetQueryID()

		// use conn as type holder for SnowflakeConnection placeholder
		var conn interface{} = sct.sc
		qStatus, err := conn.(SnowflakeConnection).GetQueryStatus(sct.sc.ctx, qid)
		if err != nil {
			t.Errorf("failed to get query status err = %s", err.Error())
			return
		}
		if qStatus == nil {
			t.Error("there was no query status returned")
			return
		}
		if qStatus.ErrorCode != "" || qStatus.ScanBytes <= 0 || qStatus.ProducedRows != 10 {
			t.Errorf("expected no error. got: %v, scan bytes: %v, produced rows: %v",
				qStatus.ErrorCode, qStatus.ScanBytes, qStatus.ProducedRows)
			return
		}
	})
}

func TestGetInvalidQueryStatus(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		sct.sc.rest.RequestTimeout = 1 * time.Second

		qStatus, err := sct.sc.checkQueryStatus(sct.sc.ctx, "1234")
		if err == nil || qStatus != nil {
			t.Error("expected an error")
		}
	})
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
	errUnknownError := errUnknownError()
	if sfe.Number != -1 || sfe.SQLState != "-1" || sfe.QueryID != "-1" {
		t.Errorf("incorrect snowflake error. expected: %v, got: %v", errUnknownError, *sfe)
	}
	if !strings.Contains(sfe.Message, "an unknown server side error occurred") {
		t.Errorf("incorrect message. expected: %v, got: %v", errUnknownError.Message, sfe.Message)
	}
}

func TestConcurrentReadOnParams(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal("Failed to parse dsn")
	}
	connector := NewConnector(SnowflakeDriver{}, *config)
	db := sql.OpenDB(connector)
	defer db.Close()
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for c := 0; c < 10; c++ {
				stmt, err := db.PrepareContext(context.Background(), "SELECT table_schema FROM information_schema.columns WHERE table_schema = ? LIMIT 1")
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
				rows.Next()
				var tableName string
				err = rows.Scan(&tableName)
				if err != nil {
					t.Error(err)
				}
				_ = rows.Close()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func postQueryTest(_ context.Context, _ *snowflakeRestful, _ *url.Values, headers map[string]string, _ []byte, _ time.Duration, _ UUID, _ *Config) (*execResponse, error) {
	return nil, errors.New("failed to get query response")
}

func postQueryFail(_ context.Context, _ *snowflakeRestful, _ *url.Values, headers map[string]string, _ []byte, _ time.Duration, _ UUID, _ *Config) (*execResponse, error) {
	dd := &execResponseData{
		QueryID:  "1eFhmhe23242kmfd540GgGre",
		SQLState: "22008",
	}
	return &execResponse{
		Data:    *dd,
		Message: "failed to get query response",
		Code:    "12345",
		Success: false,
	}, errors.New("failed to get query response")
}

func TestErrorReportingOnConcurrentFails(t *testing.T) {
	db := openDB(t)
	defer db.Close()
	var wg sync.WaitGroup
	n := 5
	wg.Add(3 * n)
	for i := 0; i < n; i++ {
		go executeQueryAndConfirmMessage(db, "SELECT * FROM TABLE_ABC", "TABLE_ABC", t, &wg)
		go executeQueryAndConfirmMessage(db, "SELECT * FROM TABLE_DEF", "TABLE_DEF", t, &wg)
		go executeQueryAndConfirmMessage(db, "SELECT * FROM TABLE_GHI", "TABLE_GHI", t, &wg)
	}
	wg.Wait()
}

func executeQueryAndConfirmMessage(db *sql.DB, query string, expectedErrorTable string, t *testing.T, wg *sync.WaitGroup) {
	defer wg.Done()
	_, err := db.Exec(query)
	message := err.(*SnowflakeError).Message
	if !strings.Contains(message, expectedErrorTable) {
		t.Errorf("QueryID: %s, Message %s ###### Expected error message table name: %s",
			err.(*SnowflakeError).QueryID, err.(*SnowflakeError).Message, expectedErrorTable)
	}
}

func TestQueryArrowStreamError(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		numrows := 50000 // approximately 10 ArrowBatch objects
		query := fmt.Sprintf(selectRandomGenerator, numrows)
		sct.sc.rest = &snowflakeRestful{
			FuncPostQuery:    postQueryTest,
			FuncCloseSession: closeSessionMock,
			TokenAccessor:    getSimpleTokenAccessor(),
			RequestTimeout:   10,
		}
		_, err := sct.sc.QueryArrowStream(sct.sc.ctx, query)
		if err == nil {
			t.Error("should have raised an error")
		}

		sct.sc.rest.FuncPostQuery = postQueryFail
		_, err = sct.sc.QueryArrowStream(sct.sc.ctx, query)
		if err == nil {
			t.Error("should have raised an error")
		}
		_, ok := err.(*SnowflakeError)
		if !ok {
			t.Fatalf("should be snowflake error. err: %v", err)
		}
	})
}

func TestExecContextError(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		sct.sc.rest = &snowflakeRestful{
			FuncPostQuery:    postQueryTest,
			FuncCloseSession: closeSessionMock,
			TokenAccessor:    getSimpleTokenAccessor(),
			RequestTimeout:   10,
		}

		_, err := sct.sc.ExecContext(sct.sc.ctx, "SELECT 1", []driver.NamedValue{})
		if err == nil {
			t.Fatalf("should have raised an error")
		}

		sct.sc.rest.FuncPostQuery = postQueryFail
		_, err = sct.sc.ExecContext(sct.sc.ctx, "SELECT 1", []driver.NamedValue{})
		if err == nil {
			t.Fatalf("should have raised an error")
		}
	})
}

func TestQueryContextError(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		sct.sc.rest = &snowflakeRestful{
			FuncPostQuery:    postQueryTest,
			FuncCloseSession: closeSessionMock,
			TokenAccessor:    getSimpleTokenAccessor(),
			RequestTimeout:   10,
		}
		_, err := sct.sc.QueryContext(sct.sc.ctx, "SELECT 1", []driver.NamedValue{})
		if err == nil {
			t.Fatalf("should have raised an error")
		}

		sct.sc.rest.FuncPostQuery = postQueryFail
		_, err = sct.sc.QueryContext(sct.sc.ctx, "SELECT 1", []driver.NamedValue{})
		if err == nil {
			t.Fatalf("should have raised an error")
		}
		_, ok := err.(*SnowflakeError)
		if !ok {
			t.Fatalf("should be snowflake error. err: %v", err)
		}
	})
}

func TestPrepareQuery(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		_, err := sct.sc.Prepare("SELECT 1")

		if err != nil {
			t.Fatalf("failed to prepare query. err: %v", err)
		}
	})
}

func TestBeginCreatesTransaction(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		tx, _ := sct.sc.Begin()
		if tx == nil {
			t.Fatal("should have created a transaction with connection")
		}
	})
}

type EmptyTransporter struct{}

func (t EmptyTransporter) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, nil
}

func TestGetTransport(t *testing.T) {
	testcases := []struct {
		name      string
		cfg       *Config
		transport http.RoundTripper
	}{
		{
			name:      "DisableOCSPChecks and InsecureMode false",
			cfg:       &Config{Account: "one", DisableOCSPChecks: false, InsecureMode: false},
			transport: SnowflakeTransport,
		},
		{
			name:      "DisableOCSPChecks true and InsecureMode false",
			cfg:       &Config{Account: "two", DisableOCSPChecks: true, InsecureMode: false},
			transport: snowflakeNoOcspTransport,
		},
		{
			name:      "DisableOCSPChecks false and InsecureMode true",
			cfg:       &Config{Account: "three", DisableOCSPChecks: false, InsecureMode: true},
			transport: snowflakeNoOcspTransport,
		},
		{
			name:      "DisableOCSPChecks and InsecureMode missing from Config",
			cfg:       &Config{Account: "four"},
			transport: SnowflakeTransport,
		},
		{
			name:      "whole Config is missing",
			cfg:       nil,
			transport: SnowflakeTransport,
		},
		{
			name:      "Using custom Transporter",
			cfg:       &Config{Account: "five", DisableOCSPChecks: true, InsecureMode: false, Transporter: EmptyTransporter{}},
			transport: EmptyTransporter{},
		},
	}
	for _, test := range testcases {
		t.Run(test.name, func(t *testing.T) {
			result := getTransport(test.cfg)
			if test.transport != result {
				t.Errorf("Failed to return the correct transport, input :%#v, expected: %v, got: %v", test.cfg, test.transport, result)
			}
		})
	}
}
