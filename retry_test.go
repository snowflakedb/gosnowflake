// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func fakeRequestFunc(_, _ string, _ io.Reader) (*http.Request, error) {
	return nil, nil
}

func emptyRequest(method string, urlStr string, body io.Reader) (*http.Request, error) {
	return http.NewRequest(method, urlStr, body)
}

type fakeHTTPError struct {
	err     string
	timeout bool
}

func (e *fakeHTTPError) Error() string   { return e.err }
func (e *fakeHTTPError) Timeout() bool   { return e.timeout }
func (e *fakeHTTPError) Temporary() bool { return true }

type fakeResponseBody struct {
	body []byte
	cnt  int
}

func (b *fakeResponseBody) Read(p []byte) (n int, err error) {
	if b.cnt == 0 {
		copy(p, b.body)
		b.cnt = 1
		return len(b.body), nil
	}
	b.cnt = 0
	return 0, io.EOF
}

func (b *fakeResponseBody) Close() error {
	return nil
}

type fakeHTTPClient struct {
	t                   *testing.T                // for assertions
	cnt                 int                       // number of retry
	success             bool                      // return success after retry in cnt times
	timeout             bool                      // timeout
	body                []byte                    // return body
	reqBody             []byte                    // last request body
	statusCode          int                       // status code
	retryNumber         int                       // consecutive number of  retries
	expectedQueryParams map[int]map[string]string // expected query params per each retry (0-based)
}

func (c *fakeHTTPClient) Do(req *http.Request) (*http.Response, error) {
	defer func() {
		c.retryNumber++
	}()
	if req != nil {
		buf := new(bytes.Buffer)
		_, err := buf.ReadFrom(req.Body)
		assertNilF(c.t, err)
		c.reqBody = buf.Bytes()
	}

	if len(c.expectedQueryParams) > 0 {
		expectedQueryParams, ok := c.expectedQueryParams[c.retryNumber]
		if ok {
			for queryParamName, expectedValue := range expectedQueryParams {
				actualValue := req.URL.Query().Get(queryParamName)
				if actualValue != expectedValue {
					c.t.Fatalf("expected query param %v to be %v, got %v", queryParamName, expectedValue, actualValue)
				}
			}
		}
	}

	c.cnt--
	if c.cnt < 0 {
		c.cnt = 0
	}
	logger.Infof("fakeHTTPClient.cnt: %v", c.cnt)

	var retcode int
	if c.success && c.cnt == 0 {
		retcode = 200
	} else {
		if c.timeout {
			// simulate timeout
			time.Sleep(time.Second * 1)
			return nil, &fakeHTTPError{
				err:     "Whatever reason (Client.Timeout exceeded while awaiting headers)",
				timeout: true,
			}
		}
		if c.statusCode != 0 {
			retcode = c.statusCode
		} else {
			retcode = 0
		}
	}

	ret := &http.Response{
		StatusCode: retcode,
		Body:       &fakeResponseBody{body: c.body},
	}
	return ret, nil
}

func TestRequestGUID(t *testing.T) {
	var ridReplacer requestGUIDReplacer
	var testURL *url.URL
	var actualURL *url.URL
	retryTime := 4

	// empty url
	testURL = &url.URL{}
	ridReplacer = newRequestGUIDReplace(testURL)
	for i := 0; i < retryTime; i++ {
		actualURL = ridReplacer.replace()
		if actualURL.String() != "" {
			t.Fatalf("empty url not replaced by an empty one, got %s", actualURL)
		}
	}

	// url with on retry id
	testURL = &url.URL{
		Path: "/" + requestIDKey + "=123-1923-9?param2=value",
	}
	ridReplacer = newRequestGUIDReplace(testURL)
	for i := 0; i < retryTime; i++ {
		actualURL = ridReplacer.replace()

		if actualURL != testURL {
			t.Fatalf("url without retry id not replaced by origin one, got %s", actualURL)
		}
	}

	// url with retry id
	// With both prefix and suffix
	prefix := "/" + requestIDKey + "=123-1923-9?" + requestGUIDKey + "="
	suffix := "?param2=value"
	testURL = &url.URL{
		Path: prefix + "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" + suffix,
	}
	ridReplacer = newRequestGUIDReplace(testURL)
	for i := 0; i < retryTime; i++ {
		actualURL = ridReplacer.replace()
		if (!strings.HasPrefix(actualURL.Path, prefix)) ||
			(!strings.HasSuffix(actualURL.Path, suffix)) ||
			len(testURL.Path) != len(actualURL.Path) {
			t.Fatalf("Retry url not replaced correctedly: \n origin: %s \n result: %s", testURL, actualURL)
		}
	}

	// With no suffix
	prefix = "/" + requestIDKey + "=123-1923-9?" + requestGUIDKey + "="
	suffix = ""
	testURL = &url.URL{
		Path: prefix + "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" + suffix,
	}
	ridReplacer = newRequestGUIDReplace(testURL)
	for i := 0; i < retryTime; i++ {
		actualURL = ridReplacer.replace()
		if (!strings.HasPrefix(actualURL.Path, prefix)) ||
			(!strings.HasSuffix(actualURL.Path, suffix)) ||
			len(testURL.Path) != len(actualURL.Path) {
			t.Fatalf("Retry url not replaced correctedly: \n origin: %s \n result: %s", testURL, actualURL)
		}

	}
	// With no prefix
	prefix = requestGUIDKey + "="
	suffix = "?param2=value"
	testURL = &url.URL{
		Path: prefix + "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" + suffix,
	}
	ridReplacer = newRequestGUIDReplace(testURL)
	for i := 0; i < retryTime; i++ {
		actualURL = ridReplacer.replace()
		if (!strings.HasPrefix(actualURL.Path, prefix)) ||
			(!strings.HasSuffix(actualURL.Path, suffix)) ||
			len(testURL.Path) != len(actualURL.Path) {
			t.Fatalf("Retry url not replaced correctedly: \n origin: %s \n result: %s", testURL, actualURL)
		}
	}
}

func TestRetryQuerySuccess(t *testing.T) {
	logger.Info("Retry N times and Success")
	client := &fakeHTTPClient{
		cnt:        3,
		success:    true,
		statusCode: 429,
		expectedQueryParams: map[int]map[string]string{
			0: {
				"retryCount":      "",
				"retryReason":     "",
				"clientStartTime": "",
			},
			1: {
				"retryCount":      "1",
				"retryReason":     "429",
				"clientStartTime": "123456",
			},
			2: {
				"retryCount":      "2",
				"retryReason":     "429",
				"clientStartTime": "123456",
			},
		},
		t: t,
	}
	urlPtr, err := url.Parse("https://fakeaccountretrysuccess.snowflakecomputing.com:443/queries/v1/query-request?" + requestIDKey + "=testid")
	assertNilF(t, err, "failed to parse the test URL")
	_, err = newRetryHTTP(context.Background(),
		client,
		emptyRequest, urlPtr, make(map[string]string), 60*time.Second, 3, constTimeProvider(123456), &Config{IncludeRetryReason: ConfigBoolTrue}).doPost().setBody([]byte{0}).execute()
	assertNilF(t, err, "failed to run retry")
	var values url.Values
	values, err = url.ParseQuery(urlPtr.RawQuery)
	assertNilF(t, err, "failed to parse the test URL")
	retry, err := strconv.Atoi(values.Get(retryCountKey))
	if err != nil {
		t.Fatalf("failed to get retry counter: %v", err)
	}
	if retry < 2 {
		t.Fatalf("not enough retry counter: %v", retry)
	}
}

func TestRetryQuerySuccessWithRetryReasonDisabled(t *testing.T) {
	logger.Info("Retry N times and Success")
	client := &fakeHTTPClient{
		cnt:        3,
		success:    true,
		statusCode: 429,
		expectedQueryParams: map[int]map[string]string{
			0: {
				"retryCount":      "",
				"retryReason":     "",
				"clientStartTime": "",
			},
			1: {
				"retryCount":      "1",
				"retryReason":     "",
				"clientStartTime": "123456",
			},
			2: {
				"retryCount":      "2",
				"retryReason":     "",
				"clientStartTime": "123456",
			},
		},
		t: t,
	}
	urlPtr, err := url.Parse("https://fakeaccountretrysuccess.snowflakecomputing.com:443/queries/v1/query-request?" + requestIDKey + "=testid")
	assertNilF(t, err, "failed to parse the test URL")
	_, err = newRetryHTTP(context.Background(),
		client,
		emptyRequest, urlPtr, make(map[string]string), 60*time.Second, 3, constTimeProvider(123456), &Config{IncludeRetryReason: ConfigBoolFalse}).doPost().setBody([]byte{0}).execute()
	assertNilF(t, err, "failed to run retry")
	var values url.Values
	values, err = url.ParseQuery(urlPtr.RawQuery)
	assertNilF(t, err, "failed to parse the test URL")
	retry, err := strconv.Atoi(values.Get(retryCountKey))
	if err != nil {
		t.Fatalf("failed to get retry counter: %v", err)
	}
	if retry < 2 {
		t.Fatalf("not enough retry counter: %v", retry)
	}
}

func TestRetryQuerySuccessWithTimeout(t *testing.T) {
	logger.Info("Retry N times and Success")
	client := &fakeHTTPClient{
		cnt:     3,
		success: true,
		timeout: true,
		expectedQueryParams: map[int]map[string]string{
			0: {
				"retryCount":  "",
				"retryReason": "",
			},
			1: {
				"retryCount":  "1",
				"retryReason": "0",
			},
			2: {
				"retryCount":  "2",
				"retryReason": "0",
			},
		},
		t: t,
	}
	urlPtr, err := url.Parse("https://fakeaccountretrysuccess.snowflakecomputing.com:443/queries/v1/query-request?" + requestIDKey + "=testid")
	assertNilF(t, err, "failed to parse the test URL")
	_, err = newRetryHTTP(context.Background(),
		client,
		emptyRequest, urlPtr, make(map[string]string), 60*time.Second, 3, constTimeProvider(123456), nil).doPost().setBody([]byte{0}).execute()
	assertNilF(t, err, "failed to run retry")
	var values url.Values
	values, err = url.ParseQuery(urlPtr.RawQuery)
	assertNilF(t, err, "failed to parse the test URL")
	retry, err := strconv.Atoi(values.Get(retryCountKey))
	if err != nil {
		t.Fatalf("failed to get retry counter: %v", err)
	}
	if retry < 2 {
		t.Fatalf("not enough retry counter: %v", retry)
	}
}

func TestRetryQueryFailWithTimeout(t *testing.T) {
	logger.Info("Retry N times until there is a timeout and Fail")
	client := &fakeHTTPClient{
		statusCode: http.StatusTooManyRequests,
		success:    false,
	}
	urlPtr, err := url.Parse("https://fakeaccountretryfail.snowflakecomputing.com:443/queries/v1/query-request?" + requestIDKey)
	assertNilF(t, err, "failed to parse the test URL")
	_, err = newRetryHTTP(context.Background(),
		client,
		emptyRequest, urlPtr, make(map[string]string), 20*time.Second, 100, defaultTimeProvider, nil).doPost().setBody([]byte{0}).execute()
	assertNotNilF(t, err, "should fail to run retry")
	var values url.Values
	values, err = url.ParseQuery(urlPtr.RawQuery)
	assertNilF(t, err, fmt.Sprintf("failed to parse the URL: %v", err))
	retry, err := strconv.Atoi(values.Get(retryCountKey))
	assertNilF(t, err, fmt.Sprintf("failed to get retry counter: %v", err))
	if retry < 2 {
		t.Fatalf("not enough retries: %v", retry)
	}
}

func TestRetryQueryFailWithMaxRetryCount(t *testing.T) {
	maxRetryCount := 3
	logger.Info("Retry 3 times until retry reaches MaxRetryCount and Fail")
	client := &fakeHTTPClient{
		statusCode: http.StatusTooManyRequests,
		success:    false,
	}
	urlPtr, err := url.Parse("https://fakeaccountretryfail.snowflakecomputing.com:443/queries/v1/query-request?" + requestIDKey)
	assertNilF(t, err, "failed to parse the test URL")
	_, err = newRetryHTTP(context.Background(),
		client,
		emptyRequest, urlPtr, make(map[string]string), 15*time.Hour, maxRetryCount, defaultTimeProvider, nil).doPost().setBody([]byte{0}).execute()
	assertNotNilF(t, err, "should fail to run retry")
	var values url.Values
	values, err = url.ParseQuery(urlPtr.RawQuery)
	if err != nil {
		t.Fatalf("failed to parse the URL: %v", err)
	}
	retryCount, err := strconv.Atoi(values.Get(retryCountKey))
	if err != nil {
		t.Fatalf("failed to get retry counter: %v", err)
	}
	if retryCount < 3 {
		t.Fatalf("not enough retries: %v; expected %v", retryCount, maxRetryCount)
	}
}

func TestRetryLoginRequest(t *testing.T) {
	logger.Info("Retry N times for timeouts and Success")
	client := &fakeHTTPClient{
		cnt:     3,
		success: true,
		timeout: true,
		t:       t,
		expectedQueryParams: map[int]map[string]string{
			0: {
				"retryCount":  "",
				"retryReason": "",
			},
			1: {
				"retryCount":  "",
				"retryReason": "",
			},
			2: {
				"retryCount":  "",
				"retryReason": "",
			},
		},
	}
	urlPtr, err := url.Parse("https://fakeaccountretrylogin.snowflakecomputing.com:443/login-request?request_id=testid")
	assertNilF(t, err, "failed to parse the test URL")
	_, err = newRetryHTTP(context.Background(),
		client,
		emptyRequest, urlPtr, make(map[string]string), 60*time.Second, 3, defaultTimeProvider, nil).doPost().setBody([]byte{0}).execute()
	assertNilF(t, err, "failed to run retry")
	var values url.Values
	values, err = url.ParseQuery(urlPtr.RawQuery)
	assertNilF(t, err, "failed to parse the test URL")
	if values.Get(retryCountKey) != "" {
		t.Fatalf("no retry counter should be attached: %v", retryCountKey)
	}
	logger.Info("Retry N times for timeouts and Fail")
	client = &fakeHTTPClient{
		success: false,
		timeout: true,
	}
	_, err = newRetryHTTP(context.Background(),
		client,
		emptyRequest, urlPtr, make(map[string]string), 5*time.Second, 3, defaultTimeProvider, nil).doPost().setBody([]byte{0}).execute()
	assertNotNilF(t, err, "should fail to run retry")
	values, err = url.ParseQuery(urlPtr.RawQuery)
	if err != nil {
		t.Fatalf("failed to parse the URL: %v", err)
	}
	if values.Get(retryCountKey) != "" {
		t.Fatalf("no retry counter should be attached: %v", retryCountKey)
	}
}

func TestRetryAuthLoginRequest(t *testing.T) {
	logger.Info("Retry N times always with newer body")
	client := &fakeHTTPClient{
		cnt:     3,
		success: true,
		timeout: true,
	}
	urlPtr, err := url.Parse("https://fakeaccountretrylogin.snowflakecomputing.com:443/login-request?request_id=testid")
	assertNilF(t, err, "failed to parse the test URL")
	execID := 0
	bodyCreator := func() ([]byte, error) {
		execID++
		return []byte(fmt.Sprintf("execID: %d", execID)), nil
	}
	_, err = newRetryHTTP(context.Background(),
		client,
		http.NewRequest, urlPtr, make(map[string]string), 60*time.Second, 3, defaultTimeProvider, nil).doPost().setBodyCreator(bodyCreator).execute()
	assertNilF(t, err, "failed to run retry")
	if lastReqBody := string(client.reqBody); lastReqBody != "execID: 3" {
		t.Fatalf("body should be updated on each request, expected: execID: 3, last body: %v", lastReqBody)
	}
}

func TestLoginRetry429(t *testing.T) {
	client := &fakeHTTPClient{
		cnt:        3,
		success:    true,
		statusCode: 429,
	}
	urlPtr, err := url.Parse("https://fakeaccountretrylogin.snowflakecomputing.com:443/login-request?request_id=testid")
	assertNilF(t, err, "failed to parse the test URL")

	_, err = newRetryHTTP(context.Background(),
		client,
		emptyRequest, urlPtr, make(map[string]string), 60*time.Second, 3, defaultTimeProvider, nil).doPost().setBody([]byte{0}).execute() // enable doRaise4XXX
	assertNilF(t, err, "failed to run retry")

	var values url.Values
	values, err = url.ParseQuery(urlPtr.RawQuery)
	assertNilF(t, err, fmt.Sprintf("failed to parse the URL: %v", err))
	if values.Get(retryCountKey) != "" {
		t.Fatalf("no retry counter should be attached: %v", retryCountKey)
	}
}

func TestIsRetryable(t *testing.T) {
	tcs := []struct {
		req      *http.Request
		res      *http.Response
		err      error
		expected bool
	}{
		{
			req:      nil,
			res:      nil,
			err:      nil,
			expected: false,
		},
		{
			req:      nil,
			res:      &http.Response{StatusCode: http.StatusBadRequest},
			err:      nil,
			expected: false,
		},
		{
			req:      &http.Request{URL: &url.URL{Path: loginRequestPath}},
			res:      nil,
			err:      nil,
			expected: false,
		},
		{
			req:      &http.Request{URL: &url.URL{Path: loginRequestPath}},
			res:      &http.Response{StatusCode: http.StatusNotFound},
			expected: false,
		},
		{
			req:      &http.Request{URL: &url.URL{Path: loginRequestPath}},
			res:      nil,
			err:      errUnknownError(),
			expected: true,
		},
		{
			req:      &http.Request{URL: &url.URL{Path: loginRequestPath}},
			res:      &http.Response{StatusCode: http.StatusTooManyRequests},
			err:      nil,
			expected: true,
		},
		{
			req:      &http.Request{URL: &url.URL{Path: queryRequestPath}},
			res:      &http.Response{StatusCode: http.StatusServiceUnavailable},
			err:      nil,
			expected: true,
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("req %v, resp %v", tc.req, tc.res), func(t *testing.T) {
			result, _ := isRetryableError(tc.req, tc.res, tc.err)
			if result != tc.expected {
				t.Fatalf("expected %v, got %v; request: %v, response: %v", tc.expected, result, tc.req, tc.res)
			}
		})
	}
}

func TestCalculateRetryWait(t *testing.T) {
	// test for randomly selected attempt and currWaitTime values
	// minSleepTime, maxSleepTime are limit values
	tcs := []struct {
		attempt      int
		currWaitTime float64
		minSleepTime float64
		maxSleepTime float64
	}{
		{
			attempt:      1,
			currWaitTime: 3.346609,
			minSleepTime: 0.326695,
			maxSleepTime: 5.019914,
		},
		{
			attempt:      2,
			currWaitTime: 4.260357,
			minSleepTime: 1.869821,
			maxSleepTime: 6.390536,
		},
		{
			attempt:      3,
			currWaitTime: 7.857728,
			minSleepTime: 3.928864,
			maxSleepTime: 11.928864,
		},
		{
			attempt:      4,
			currWaitTime: 7.249255,
			minSleepTime: 3.624628,
			maxSleepTime: 19.624628,
		},
		{
			attempt:      5,
			currWaitTime: 23.598257,
			minSleepTime: 11.799129,
			maxSleepTime: 43.799129,
		},
		{
			attempt:      8,
			currWaitTime: 27.088613,
			minSleepTime: 13.544306,
			maxSleepTime: 269.544306,
		},
		{
			attempt:      10,
			currWaitTime: 30.879329,
			minSleepTime: 15.439664,
			maxSleepTime: 1039.439664,
		},
		{
			attempt:      12,
			currWaitTime: 39.919798,
			minSleepTime: 19.959899,
			maxSleepTime: 4115.959899,
		},
		{
			attempt:      15,
			currWaitTime: 33.750758,
			minSleepTime: 16.875379,
			maxSleepTime: 32784.875379,
		},
		{
			attempt:      20,
			currWaitTime: 32.357793,
			minSleepTime: 16.178897,
			maxSleepTime: 1048592.178897,
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("attmept: %v", tc.attempt), func(t *testing.T) {
			result := defaultWaitAlgo.calculateWaitBeforeRetryForAuthRequest(tc.attempt, time.Duration(tc.currWaitTime*float64(time.Second)))
			assertBetweenE(t, result.Seconds(), tc.minSleepTime, tc.maxSleepTime)
		})
	}
}

func TestCalculateRetryWaitForNonAuthRequests(t *testing.T) {
	// test for randomly selected currWaitTime values
	// maxSleepTime is the limit value
	tcs := []struct {
		currWaitTime float64
		maxSleepTime float64
	}{
		{
			currWaitTime: 3.346609,
			maxSleepTime: 10.039827,
		},
		{
			currWaitTime: 4.260357,
			maxSleepTime: 12.781071,
		},
		{
			currWaitTime: 5.154231,
			maxSleepTime: 15.462693,
		},
		{
			currWaitTime: 7.249255,
			maxSleepTime: 16,
		},
		{
			currWaitTime: 23.598257,
			maxSleepTime: 16,
		},
	}

	for _, tc := range tcs {
		defaultMinSleepTime := 1
		t.Run(fmt.Sprintf("currWaitTime: %v", tc.currWaitTime), func(t *testing.T) {
			result := defaultWaitAlgo.calculateWaitBeforeRetry(time.Duration(tc.currWaitTime) * time.Second)
			assertBetweenInclusiveE(t, result.Seconds(), float64(defaultMinSleepTime), tc.maxSleepTime)
		})
	}
}
