package gosnowflake

import (
	"context"
	"net/http"
	"net/url"
	"testing"
)

type DummyTransport struct {
	postRequests int
	getRequests  int
}

func (t *DummyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path == "" {
		switch r.Method {
		case http.MethodGet:
			t.getRequests++
		case http.MethodPost:
			t.postRequests++
		}
		return &http.Response{StatusCode: 200}, nil
	}
	return createTestNoRevocationTransport().RoundTrip(r)
}

func TestInternalClient(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("failed to parse dsn. err: %v", err)
	}
	transport := DummyTransport{}
	config.Transporter = &transport
	driver := SnowflakeDriver{}
	db, err := driver.OpenWithConfig(context.Background(), *config)
	if err != nil {
		t.Fatalf("failed to open with config. config: %v, err: %v", config, err)
	}

	internalClient := (db.(*snowflakeConn)).internal
	resp, err := internalClient.Get(context.Background(), &url.URL{}, make(map[string]string), 0)
	if err != nil || resp.StatusCode != 200 {
		t.Fail()
	}
	if transport.getRequests != 1 {
		t.Fatalf("Expected exactly one GET request, got %v", transport.getRequests)
	}

	resp, err = internalClient.Post(context.Background(), &url.URL{}, make(map[string]string), make([]byte, 0), 0, defaultTimeProvider)
	if err != nil || resp.StatusCode != 200 {
		t.Fail()
	}
	if transport.postRequests != 1 {
		t.Fatalf("Expected exactly one POST request, got %v", transport.postRequests)
	}

	db.Close()
}
