// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"testing"
)

func TestPostBackURL(t *testing.T) {
	c := `<html><form id="1" action="https&#x3a;&#x2f;&#x2f;abc.com&#x2f;"></form></html>`
	urlp, err := postBackURL([]byte(c))
	if err != nil {
		t.Fatalf("failed to get URL. err: %v, %v", err, c)
	}
	if urlp != "https://abc.com/" {
		t.Errorf("failed to get URL. got: %v, %v", urlp, c)
	}
	c = `<html></html>`
	urlp, err = postBackURL([]byte(c))
	if err == nil {
		t.Fatalf("should have failed")
	}
	c = `<html><form id="1"/></html>`
	urlp, err = postBackURL([]byte(c))
	if err == nil {
		t.Fatalf("should have failed")
	}
	c = `<html><form id="1" action="https&#x3a;&#x2f;&#x2f;abc.com&#x2f;/></html>`
	urlp, err = postBackURL([]byte(c))
	if err == nil {
		t.Fatalf("should have failed")
	}
}

type tcIsPrefixEqual struct {
	url1   string
	url2   string
	result bool
	err    error
}

func TestIsPrefixEqual(t *testing.T) {
	testcases := []tcIsPrefixEqual{
		{url1: "https://abc.com/", url2: "https://abc.com", result: true},
		{url1: "https://def.com/", url2: "https://abc.com", result: false},
		{url1: "http://def.com", url2: "https://def.com", result: false},
		{url1: "afdafdafadfs", url2: "https://def.com", result: false},
		{url1: "http://def.com", url2: "afdafafd", result: false},
		{url1: "https://abc.com", url2: "https://abc.com:443/", result: true},
	}
	for _, test := range testcases {
		r, err := isPrefixEqual(test.url1, test.url2)
		if test.err != nil {
			if err == nil {
				t.Errorf("should have failed. url1: %v, url2: %v, got: %v, expected err: %v", test.url1, test.url2, r, test.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("failed. url1: %v, url2: %v, expected: %v, err: %v", test.url1, test.url2, test.result, err)
		} else {
			if r && !test.result || !r && test.result {
				t.Errorf("failed. url1: %v, url2: %v, expected: %v, got: %v", test.url1, test.url2, test.result, r)
			}
		}
	}
}
