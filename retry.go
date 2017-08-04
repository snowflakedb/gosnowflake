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

	"context"

	"sync"

	"github.com/golang/glog"
)

var random *rand.Rand

func init() {
	random = rand.New(rand.NewSource(time.Now().UnixNano()))
}

type waitAlgo struct {
	mutex *sync.Mutex   // required for random.Int63n
	base  time.Duration // base wait time
	cap   time.Duration // maximum wait time
}

func randSecondDuration(n time.Duration) time.Duration {
	return time.Duration(random.Int63n(int64(n/time.Second))) * time.Second
}

// exponential backoff (experimental)
func (w *waitAlgo) exp(attempt int, sleep time.Duration) time.Duration {
	return durationMax(durationMin(1<<uint(attempt)*w.base, w.cap), 1*time.Second)
}

// full jitter backoff (experimental)
func (w *waitAlgo) fullJitter(attempt int, sleep time.Duration) time.Duration {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return randSecondDuration(w.exp(attempt, sleep))
}

// equal jitter backoff (experimental)
func (w *waitAlgo) eqJitter(attempt int, sleep time.Duration) time.Duration {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	t := w.exp(attempt, sleep) / time.Duration(2)
	return t + randSecondDuration(t)
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

func retryHTTP(
	ctx context.Context,
	client clientInterface,
	req requestFunc,
	method string,
	fullURL string,
	headers map[string]string,
	body []byte,
	timeout time.Duration) (res *http.Response, err error) {
	totalTimeout := timeout
	glog.V(2).Infof("retryHTTP.totalTimeout: %v", totalTimeout)
	retryCounter := 0
	sleepTime := time.Duration(0)
	for {
		req, err := req(method, fullURL, bytes.NewReader(body))
		if req != nil {
			req = req.WithContext(ctx)
		}
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
		if err == ErrCanceled {
			// user cancel only. not context.Canceled
			break
		}
		// cannot just return 4xx and 5xx status as the error can be sporadic. retry often helps.
		if err != nil {
			glog.V(2).Infof(
				"failed http connection. no response is returned. err: %v. retrying.\n", err)
		} else {
			glog.V(2).Infof(
				"failed http connection. HTTP Status: %v. retrying.\n", res.StatusCode)
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
				return nil, fmt.Errorf("timeout. HTTP Status: %v. Hanging?", res.StatusCode)
			}
		}
		retryCounter++
		glog.V(2).Infof("sleeping %v. to timeout: %v. retrying", sleepTime, totalTimeout)
		time.Sleep(sleepTime)
	}
	return res, err
}
