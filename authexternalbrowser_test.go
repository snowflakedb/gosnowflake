// Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"testing"
)

func TestGetTokenFromResponseFail(t *testing.T) {
	response := "GET /?fakeToken=fakeEncodedSamlToken HTTP/1.1\r\n" +
		"Host: localhost:54001\r\n" +
		"Connection: keep-alive\r\n" +
		"Upgrade-Insecure-Requests: 1\r\n" +
		"User-Agent: userAgentStr\r\n" +
		"Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8\r\n" +
		"Referer: https://myaccount.snowflakecomputing.com/fed/login\r\n" +
		"Accept-Encoding: gzip, deflate, br\r\n" +
		"Accept-Language: en-US,en;q=0.9\r\n\r\n"

	_, err := getTokenFromResponse(response)
	if err == nil {
		t.Errorf("Should have failed parsing the malformed response.")
	}
}

func TestGetTokenFromResponse(t *testing.T) {
	response := "GET /?token=GETtokenFromResponse HTTP/1.1\r\n" +
		"Host: localhost:54001\r\n" +
		"Connection: keep-alive\r\n" +
		"Upgrade-Insecure-Requests: 1\r\n" +
		"User-Agent: userAgentStr\r\n" +
		"Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8\r\n" +
		"Referer: https://myaccount.snowflakecomputing.com/fed/login\r\n" +
		"Accept-Encoding: gzip, deflate, br\r\n" +
		"Accept-Language: en-US,en;q=0.9\r\n\r\n"

	expected := "GETtokenFromResponse"

	token, err := getTokenFromResponse(response)
	if err != nil {
		t.Errorf("Failed to get the token. Err: %#v", err)
	}
	if token != expected {
		t.Errorf("Expected: %s, found: %s", expected, token)
	}
}
