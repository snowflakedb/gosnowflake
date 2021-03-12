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
	httpClient.setClient(&fakeHTTPClient{success: true})

	_, err := httpClient.Get("test_path", getHeaders(), defaultClientTimeout)
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
	httpClient.setClient(&fakeHTTPClient{success: true})

	_, err := httpClient.Post("test_path", getHeaders(), nil, defaultClientTimeout)
	if err != nil {
		t.Error(err)
	}
}
