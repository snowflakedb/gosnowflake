package gosnowflake

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

type DummyTransport struct {
	postRequests int
	getRequests  int
}

func (t *DummyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// Return successful response with mock body for different request types
	var responseBody io.ReadCloser
	if strings.Contains(r.URL.Path, "login-request") {
		// Mock successful login response with correct types (don't count auth requests)
		responseBody = io.NopCloser(strings.NewReader(`{"success":true,"data":{"token":"mock-token","sessionId":12345}}`))
	} else if strings.Contains(r.URL.Path, "telemetry") {
		// Mock telemetry response (don't count telemetry requests)
		responseBody = io.NopCloser(strings.NewReader(`{"success":true}`))
	} else {
		// Count only explicit API calls, not authentication or telemetry
		if r.Method == "GET" {
			t.getRequests++
		} else if r.Method == "POST" {
			t.postRequests++
		}
		responseBody = io.NopCloser(strings.NewReader(`{"success":true}`))
	}

	return &http.Response{
		StatusCode: 200,
		Body:       responseBody,
		Header:     make(http.Header),
	}, nil
}

func TestInternalClient(t *testing.T) {
	fakeDSN := "testuser:testpass@testaccount.snowflakecomputing.com:443/testdb/testschema?warehouse=testwh&role=testrole"
	config, err := ParseDSN(fakeDSN)
	if err != nil {
		t.Fatalf("failed to parse dsn. err: %v", err)
	}
	config.Authenticator = AuthTypeSnowflake
	config.PrivateKey = nil
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
