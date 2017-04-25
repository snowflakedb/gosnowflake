// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"

	"github.com/golang/glog"
)

var random *rand.Rand

func init() {
	random = rand.New(rand.NewSource(time.Now().UnixNano()))
}

type waitTime func(int, int) int
type waitAlgo struct {
	base int64
	cap  int64
}

// exponential backoff
func (w *waitAlgo) exp(attempt int, sleep int) int {
	return intMax(int(intMin64(int64(1<<uint(attempt)*w.base), w.cap)), 1)
}

// full jitter backoff
func (w *waitAlgo) fullJitter(attempt int, sleep int) int {
	return random.Intn(w.exp(attempt, sleep))
}

// equal jitter backoff
func (w *waitAlgo) eqJitter(attempt int, sleep int) int {
	t := w.exp(attempt, sleep)
	return t/2 + random.Intn(t/2)
}

// decorrelated jitter backoff
func (w *waitAlgo) decorr(attempt int, sleep int) int {
	t := int64(sleep*3) - w.base
	switch {
	case t > 0:
		return int(intMin64(w.cap, random.Int63n(t)+w.base))
	case t < 0:
		return int(intMin64(w.cap, random.Int63n(-t)+int64(sleep*3)))
	}
	return int(w.base)
}

var defaultWaitAlgo = &waitAlgo{5, 160}

type requestFunc func(method, urlStr string, body io.Reader) (*http.Request, error)

type clientInterface interface {
	Do(req *http.Request) (*http.Response, error)
}

func retryHTTP(
	client clientInterface,
	req requestFunc,
	method string,
	fullURL string,
	headers map[string]string,
	body []byte,
	timeout time.Duration) (res *http.Response, err error) {
	totalTimeout := int64(timeout.Seconds())
	glog.V(2).Infof("totalTimeout: %v", totalTimeout)
	retryCounter := 0
	sleepTime := 0
	for {
		req, err := req(method, fullURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		res, err = client.Do(req)
		if err == nil && res.StatusCode == http.StatusOK {
			break
		}
		if err != nil {
			glog.V(2).Infof(
				"failed http connection. no response is returned. err: %v. retrying.\n", err)
		} else {
			glog.V(2).Infof(
				"failed http connection. HTTP Status: %v. retrying.\n", res.StatusCode)
		}
		sleepTime = defaultWaitAlgo.decorr(retryCounter, sleepTime)

		if totalTimeout > 0 {
			glog.V(2).Infof("to timeout: %v", totalTimeout)
			// if any timeout is set
			totalTimeout -= int64(sleepTime)
			if totalTimeout <= 0 {
				if err != nil {
					return nil, fmt.Errorf("timeout. previous err: %v. Hanging?", err)
				}
				return nil, fmt.Errorf("timeout. previous HTTP Status: %v. Hanging?", res.StatusCode)
			}
		}
		retryCounter++
		glog.V(2).Infof("sleeping %v(s). to timeout: %v. retrying", sleepTime, totalTimeout)
		time.Sleep(time.Duration(int64(sleepTime) * int64(time.Second)))
	}
	return res, err
}
