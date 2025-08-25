package gosnowflake

import (
	"net/http"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

type countingRoundTripper struct {
	delegate     http.RoundTripper
	getReqCount  map[string]int
	postReqCount map[string]int
}

func newCountingRoundTripper(delegate http.RoundTripper) *countingRoundTripper {
	return &countingRoundTripper{
		delegate:     delegate,
		getReqCount:  make(map[string]int),
		postReqCount: make(map[string]int),
	}
}

func (crt *countingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	switch req.Method {
	case http.MethodGet:
		crt.getReqCount[req.URL.String()]++
	case http.MethodPost:
		crt.postReqCount[req.URL.String()]++
	}

	return crt.delegate.RoundTrip(req)
}

func (crt *countingRoundTripper) reset() {
	crt.getReqCount = make(map[string]int)
	crt.postReqCount = make(map[string]int)
}

func (crt *countingRoundTripper) totalRequestsByPath(urlPath string) int {
	total := 0
	for url, reqs := range crt.getReqCount {
		if strings.Contains(url, urlPath) {
			total += reqs
		}
	}
	for url, reqs := range crt.postReqCount {
		if strings.Contains(url, urlPath) {
			total += reqs
		}
	}
	return total
}

func (crt *countingRoundTripper) totalRequests() int {
	total := 0
	for _, reqs := range crt.getReqCount {
		total += reqs
	}
	for _, reqs := range crt.postReqCount {
		total += reqs
	}
	return total
}

type blockingRoundTripper struct {
	delegate         http.RoundTripper
	defaultBlockTime time.Duration
	pathBlockTime    map[string]time.Duration
}

func newBlockingRoundTripper(delegate http.RoundTripper, defaultBlockTime time.Duration) *blockingRoundTripper {
	return &blockingRoundTripper{
		delegate:         delegate,
		defaultBlockTime: defaultBlockTime,
		pathBlockTime:    make(map[string]time.Duration),
	}
}

func (brt *blockingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if blockTime, exists := brt.pathBlockTime[req.URL.Path]; exists {
		time.Sleep(blockTime)
	} else if brt.defaultBlockTime != 0 {
		time.Sleep(brt.defaultBlockTime)
	}
	return brt.delegate.RoundTrip(req)
}

func (brt *blockingRoundTripper) setPathBlockTime(path string, blockTime time.Duration) {
	brt.pathBlockTime[path] = blockTime
}

func (brt *blockingRoundTripper) reset() {
	brt.pathBlockTime = make(map[string]time.Duration)
}

func skipOnMissingHome(t *testing.T) {
	if (runtime.GOOS == "linux" || runtime.GOOS == "darwin") && os.Getenv("HOME") == "" {
		t.Skip("skipping on missing HOME environment variable")
	}
}
