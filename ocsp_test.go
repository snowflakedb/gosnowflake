// Package gosnowflake is a utility package for Go Snowflake Driver
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
)

func TestOCSP(t *testing.T) {
	targetURL := "https://sfctest0.snowflakecomputing.com/" // testaccount

	c := &http.Client{
		Transport: SnowflakeTransportTest,
		Timeout:   30 * time.Second,
	}
	req, err := http.NewRequest("GET", targetURL, bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("fail to create a request. err: %v", err)
	}
	res, err := c.Do(req)
	if err != nil {
		t.Fatalf("failed to GET contents. err: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("failed to get 200: %v", res.StatusCode)
	}
	_, err = ioutil.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("failed to read content body for %v", targetURL)
	}
}
