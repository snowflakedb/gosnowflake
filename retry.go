// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"context"

	"sync"
)

var random *rand.Rand

func init() {
	random = rand.New(rand.NewSource(time.Now().UnixNano()))
}

// request_guid is attached to every request against Snowflake
const requestGUIDKey string = "request_guid"

// retryCounter is attached to query-request from the second time
const retryCounterParam string = "retryCounter"

// This class takes in an url during construction and replace the
// value of request_guid every time the replace() is called
// When the url does not contain request_guid, just return the original
// url
type requestGUIDReplacerI interface {
	// replace the url with new ID
	replace() *url.URL
}

// Make requestGUIDReplacer given a url string
func makeRequestGUIDReplacer(urlPtr *url.URL) requestGUIDReplacerI {
	_, err := url.ParseQuery(urlPtr.RawQuery)
	if err != nil {
		return &transientReplacer{urlPtr}
	}
	return &requestGUIDReplacer{urlPtr}
}

// this replacer does nothing but replace the url
type transientReplacer struct {
	urlPtr *url.URL
}

func (replacer *transientReplacer) replace() *url.URL {
	return replacer.urlPtr
}

/*
requestGUIDReplacer is a one-shot object that is created out of the retry loop and
called with replace to change the retry_guid's value upon every retry
*/
type requestGUIDReplacer struct {
	urlPtr *url.URL
}

/**
This function would replace they value of the requestGUIDKey in a url with a newly
generated uuid
*/
func (replacer *requestGUIDReplacer) replace() *url.URL {
	vs, err := url.ParseQuery(replacer.urlPtr.RawQuery)
	if err != nil {
		return replacer.urlPtr
	}
	if len(vs.Get(requestGUIDKey)) == 0 {
		return replacer.urlPtr
	}
	vs.Del(requestGUIDKey)
	vs.Add(requestGUIDKey, uuid.New().String())
	replacer.urlPtr.RawQuery = vs.Encode()
	return replacer.urlPtr
}

type retryUpdater interface {
	replaceOrAdd(retry int) *url.URL
}

type retryUpdate struct {
	TargetURL *url.URL
}

func (r *retryUpdate) replaceOrAdd(retry int) *url.URL {
	if !strings.HasPrefix(r.TargetURL.Path, queryRequestPath) {
		return r.TargetURL
	}

	vs, err := url.ParseQuery(r.TargetURL.RawQuery)
	if err != nil {
		return r.TargetURL
	}
	vs.Del(retryCounterParam)
	vs.Add(retryCounterParam, strconv.Itoa(retry))
	r.TargetURL.RawQuery = vs.Encode()
	return r.TargetURL
}

func newRetryUpdate(targetURL *url.URL) retryUpdater {
	return &retryUpdate{
		TargetURL: targetURL,
	}
}

type waitAlgo struct {
	mutex *sync.Mutex   // required for random.Int63n
	base  time.Duration // base wait time
	cap   time.Duration // maximum wait time
}

func randSecondDuration(n time.Duration) time.Duration {
	return time.Duration(random.Int63n(int64(n/time.Second))) * time.Second
}

// decorrelated jitter backoff
func (w *waitAlgo) decorr(attempt int, sleep time.Duration) time.Duration {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	t := 3*sleep - w.base
	switch {
	case t > 0:
		return durationMin(w.cap, randSecondDuration(t)+w.base)
	case t < 0:
		return durationMin(w.cap, randSecondDuration(-t)+3*sleep)
	}
	return w.base
}

var defaultWaitAlgo = &waitAlgo{
	mutex: &sync.Mutex{},
	base:  5 * time.Second,
	cap:   160 * time.Second,
}

type requestFunc func(method, urlStr string, body io.Reader) (*http.Request, error)

type clientInterface interface {
	Do(req *http.Request) (*http.Response, error)
}

type retryHTTP struct {
	ctx      context.Context
	client   clientInterface
	req      requestFunc
	method   string
	fullURL  *url.URL
	headers  map[string]string
	body     []byte
	timeout  time.Duration
	raise4XX bool
}

func newRetryHTTP(ctx context.Context,
	client clientInterface,
	req requestFunc,
	fullURL *url.URL,
	headers map[string]string,
	timeout time.Duration) *retryHTTP {
	instance := retryHTTP{}
	instance.ctx = ctx
	instance.client = client
	instance.req = req
	instance.method = "GET"
	instance.fullURL = fullURL
	instance.headers = headers
	instance.body = nil
	instance.timeout = timeout
	instance.raise4XX = false
	return &instance
}

func (r *retryHTTP) doRaise4XX(raise4XX bool) *retryHTTP {
	r.raise4XX = raise4XX
	return r
}

func (r *retryHTTP) doPost() *retryHTTP {
	r.method = "POST"
	return r
}

func (r *retryHTTP) setBody(body []byte) *retryHTTP {
	r.body = body
	return r
}

func (r *retryHTTP) execute() (res *http.Response, err error) {
	totalTimeout := r.timeout
	glog.V(2).Infof("retryHTTP.totalTimeout: %v", totalTimeout)
	retryCounter := 0
	sleepTime := time.Duration(0)

	var rIDReplacer requestGUIDReplacerI
	var rUpdater retryUpdater

	for {
		req, err := r.req(r.method, r.fullURL.String(), bytes.NewReader(r.body))
		if err != nil {
			return nil, err
		}
		if req != nil {
			// req can be nil in tests
			req = req.WithContext(r.ctx)
		}
		for k, v := range r.headers {
			req.Header.Set(k, v)
		}
		res, err = r.client.Do(req)
		if err == nil && res.StatusCode == http.StatusOK {
			// exit if success
			break
		}
		if r.raise4XX && res != nil && res.StatusCode >= 400 && res.StatusCode < 500 {
			// abort connection if raise4XX flag is enabled and the range of HTTP status code are 4XX.
			// This is currently used for Snowflake login. The caller must generate an error object based on HTTP status.
			break
		}

		// context cancel or timeout
		if err != nil {
			urlError, isURLError := err.(*url.Error)
			if isURLError &&
				(urlError.Err == context.DeadlineExceeded || urlError.Err == context.Canceled) {
				return res, urlError.Err
			}
		}

		// cannot just return 4xx and 5xx status as the error can be sporadic. run often helps.
		if err != nil {
			glog.V(2).Infof(
				"failed http connection. no response is returned. err: %v. retrying...\n", err)
		} else {
			glog.V(2).Infof(
				"failed http connection. HTTP Status: %v. retrying...\n", res.StatusCode)
		}
		// uses decorrelated jitter backoff
		sleepTime = defaultWaitAlgo.decorr(retryCounter, sleepTime)

		if totalTimeout > 0 {
			glog.V(2).Infof("to timeout: %v", totalTimeout)
			// if any timeout is set
			totalTimeout -= sleepTime
			if totalTimeout <= 0 {
				if err != nil {
					return nil, fmt.Errorf("timeout. err: %v. Hanging?", err)
				}
				if res != nil {
					return nil, fmt.Errorf("timeout. HTTP Status: %v. Hanging?", res.StatusCode)
				}
				return nil, errors.New("timeout. Hanging?")
			}
		}
		retryCounter++
		if rIDReplacer == nil {
			rIDReplacer = makeRequestGUIDReplacer(r.fullURL)
		}
		r.fullURL = rIDReplacer.replace()
		if rUpdater == nil {
			rUpdater = newRetryUpdate(r.fullURL)
		}
		r.fullURL = rUpdater.replaceOrAdd(retryCounter)
		glog.V(2).Infof("sleeping %v. to timeout: %v. retrying", sleepTime, totalTimeout)

		await := time.NewTimer(sleepTime)
		select {
		case <-await.C:
			// retry the request
		case <-r.ctx.Done():
			await.Stop()
			return res, r.ctx.Err()
		}
	}
	return res, err
}
