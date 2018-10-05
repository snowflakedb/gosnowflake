package gosnowflake

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func fakeRequestFunc(_, _ string, _ io.Reader) (*http.Request, error) {
	return nil, nil
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
	cnt     int    // number of retry
	success bool   // return success after retry in cnt times
	timeout bool   // timeout
	body    []byte // return body
}

func (c *fakeHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.cnt--
	if c.cnt < 0 {
		c.cnt = 0
	}
	glog.V(2).Infof("fakeHTTPClient.cnt: %v", c.cnt)

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
		retcode = 0
	}

	ret := &http.Response{
		StatusCode: retcode,
		Body:       &fakeResponseBody{body: c.body},
	}
	return ret, nil
}

func TestRetryId(t *testing.T) {
	var ridReplacer retryIDReplacerI
	var testURL string
	var actualURL string
	retryTime := 4

	// empty url
	testURL = ""
	ridReplacer = makeRetryIDReplacer(testURL)
	for i := 0; i < retryTime; i++ {
		actualURL = ridReplacer.replaceRetryID()
		if actualURL != "" {
			t.Fatalf("empty url not replaced by an empty one, got %s", actualURL)
		}
	}

	// url with on retry id
	testURL = "/requestId=123-1923-9?param2=value"
	ridReplacer = makeRetryIDReplacer(testURL)
	for i := 0; i < retryTime; i++ {
		actualURL = ridReplacer.replaceRetryID()

		if actualURL != testURL {
			t.Fatalf("url without retry id not replaced by origin one, got %s", actualURL)
		}
	}

	// url with retry id
	// With both prefix and suffix
	prefix := "/requestId=123-1923-9?" + retryKey + "="
	suffix := "?param2=value"
	testURL = prefix + "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" + suffix
	ridReplacer = makeRetryIDReplacer(testURL)
	for i := 0; i < retryTime; i++ {
		actualURL = ridReplacer.replaceRetryID()
		if (!strings.HasPrefix(actualURL, prefix)) ||
			(!strings.HasSuffix(actualURL, suffix)) ||
			len(testURL) != len(actualURL) {
			t.Fatalf("Retry url not replaced correctedly: \n origin: %s \n result: %s", testURL, actualURL)
		}
	}

	// With no suffix
	prefix = "/requestId=123-1923-9?" + retryKey + "="
	suffix = ""
	testURL = prefix + "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" + suffix
	ridReplacer = makeRetryIDReplacer(testURL)
	for i := 0; i < retryTime; i++ {
		actualURL = ridReplacer.replaceRetryID()
		if (!strings.HasPrefix(actualURL, prefix)) ||
			(!strings.HasSuffix(actualURL, suffix)) ||
			len(testURL) != len(actualURL) {
			t.Fatalf("Retry url not replaced correctedly: \n origin: %s \n result: %s", testURL, actualURL)
		}

	}
	// With no prefix
	prefix = retryKey + "="
	suffix = "?param2=value"
	testURL = prefix + "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" + suffix
	ridReplacer = makeRetryIDReplacer(testURL)
	for i := 0; i < retryTime; i++ {
		actualURL = ridReplacer.replaceRetryID()
		if (!strings.HasPrefix(actualURL, prefix)) ||
			(!strings.HasSuffix(actualURL, suffix)) ||
			len(testURL) != len(actualURL) {
			t.Fatalf("Retry url not replaced correctedly: \n origin: %s \n result: %s", testURL, actualURL)
		}
	}
}

func TestRetry(t *testing.T) {
	glog.V(2).Info("Retry N times and Success")
	client := &fakeHTTPClient{
		cnt:     3,
		success: true,
	}
	_, err := retryHTTP(context.TODO(),
		client,
		fakeRequestFunc, "POST", "", make(map[string]string), []byte{0}, 60*time.Second, false)
	if err != nil {
		t.Fatal("failed to run retry")
	}

	glog.V(2).Info("Retry N times and Fail")
	client = &fakeHTTPClient{
		cnt:     10,
		success: false,
	}
	_, err = retryHTTP(context.TODO(),
		client,
		fakeRequestFunc, "POST", "", make(map[string]string), []byte{0}, 10*time.Second, false)
	if err == nil {
		t.Fatal("should fail to run retry")
	}

	glog.V(2).Info("Retry N times for timeouts and Success")
	client = &fakeHTTPClient{
		cnt:     3,
		success: true,
		timeout: true,
	}
	_, err = retryHTTP(context.TODO(),
		client,
		fakeRequestFunc, "POST", "", make(map[string]string), []byte{0}, 60*time.Second, false)
	if err != nil {
		t.Fatal("failed to run retry")
	}
	glog.V(2).Info("Retry N times for timeouts and Fail")
	client = &fakeHTTPClient{
		cnt:     10,
		success: false,
		timeout: true,
	}
	_, err = retryHTTP(context.TODO(),
		client,
		fakeRequestFunc, "POST", "", make(map[string]string), []byte{0}, 10*time.Second, false)
	if err == nil {
		t.Fatal("should fail to run retry")
	}
}
