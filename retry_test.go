package gosnowflake

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/golang/glog"
	"context"
)

func fakeRequestFunc(method, urlStr string, body io.Reader) (*http.Request, error) {
	return nil, nil
}

type fakeHTTPError struct {
	err     string
	timeout bool
}

func (e *fakeHTTPError) Error() string   { return e.err }
func (e *fakeHTTPError) Timeout() bool   { return e.timeout }
func (e *fakeHTTPError) Temporary() bool { return true }

type fakeClient struct {
	cnt     int  // number of retry
	success bool // return success after retry in cnt times
	timeout bool // timeout
}

func (c *fakeClient) Do(req *http.Request) (*http.Response, error) {
	c.cnt--
	glog.V(2).Infof("fakeClient.cnt: %v", c.cnt)

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
	}
	return ret, nil
}

func TestRetry(t *testing.T) {
	glog.V(2).Info("Retry N times and Success")
	client := &fakeClient{
		cnt:     3,
		success: true,
	}
	_, err := retryHTTP(context.TODO(),
		client,
		fakeRequestFunc, "POST", "", make(map[string]string), []byte{0}, 60*time.Second)
	if err != nil {
		t.Fatal("failed to run retry")
	}

	glog.V(2).Info("Retry N times and Fail")
	client = &fakeClient{
		cnt:     10,
		success: false,
	}
	_, err = retryHTTP(context.TODO(),
		client,
		fakeRequestFunc, "POST", "", make(map[string]string), []byte{0}, 10*time.Second)
	if err == nil {
		t.Fatal("should fail to run retry")
	}

	glog.V(2).Info("Retry N times for timeouts and Success")
	client = &fakeClient{
		cnt:     3,
		success: true,
		timeout: true,
	}
	_, err = retryHTTP(context.TODO(),
		client,
		fakeRequestFunc, "POST", "", make(map[string]string), []byte{0}, 60*time.Second)
	if err != nil {
		t.Fatal("failed to run retry")
	}
	glog.V(2).Info("Retry N times for timeouts and Fail")
	client = &fakeClient{
		cnt:     10,
		success: false,
		timeout: true,
	}
	_, err = retryHTTP(context.TODO(),
		client,
		fakeRequestFunc, "POST", "", make(map[string]string), []byte{0}, 10*time.Second)
	if err == nil {
		t.Fatal("should fail to run retry")
	}
}
