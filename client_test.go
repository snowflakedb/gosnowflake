// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"net/http"
	"net/url"
	"testing"
)

type DummyTransport struct {
}

func (t *DummyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200}, nil
}

func TestInternalClient(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("failed to parse dsn. dsn: %v, err: %v", dsn, err)
	}
	config.Transporter = &DummyTransport{}
	driver := SnowflakeDriver{}
	db, err := driver.OpenWithConfig(context.Background(), *config)
	if err != nil {
		t.Fatalf("failed to open with config. config: %v, err: %v", config, err)
	}

	internalClient := (db.(*snowflakeConn)).newInternalClient()
	resp, err := internalClient.Get(context.Background(), &url.URL{}, make(map[string]string), 0)
	if err != nil || resp.StatusCode != 200 {
		t.Fail()
	}

	resp, err = internalClient.Post(context.Background(), &url.URL{}, make(map[string]string), make([]byte, 0), 0, false)
	if err != nil || resp.StatusCode != 200 {
		t.Fail()
	}
	db.Close()
}
