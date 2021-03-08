// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"testing"
)

func TestHTTPClientGet(t *testing.T) {
	client := NewClient().client
	httpClient, ok := client.(*HTTPClient)
	if !ok {
		t.Fatal("failed to convert to HTTP client")
	}

	sf := new(SnowflakeDriver)
	c, _ := sf.Open(dsn)
	conn := c.(*snowflakeConn)
	httpClient.SetConfig(conn.cfg)

	_, err := httpClient.Get(testPath, getHeaders(), defaultClientTimeout)
	if err != nil {
		t.Error(err)
	}
}

func TestHTTPClientPost(t *testing.T) {
	client := NewClient().client
	httpClient, ok := client.(*HTTPClient)
	if !ok {
		t.Fatal("failed to convert to HTTP client")
	}

	sf := new(SnowflakeDriver)
	c, _ := sf.Open(dsn)
	conn := c.(*snowflakeConn)
	httpClient.SetConfig(conn.cfg)

	_, err := httpClient.Post(testPath, getHeaders(), nil, defaultClientTimeout)
	if err != nil {
		t.Error(err)
	}
}