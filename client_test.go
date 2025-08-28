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
	assertNilF(t, err, "failed to parse dsn")
	transport := DummyTransport{}
	config.Transporter = &transport
	driver := SnowflakeDriver{}
	db, err := driver.OpenWithConfig(context.Background(), *config)
	assertNilF(t, err, "failed to open with config")

	internalClient := (db.(*snowflakeConn)).internal
	resp, err := internalClient.Get(context.Background(), &url.URL{}, make(map[string]string), 0)
	assertNilF(t, err, "GET request should succeed")
	assertEqualF(t, resp.StatusCode, 200, "GET response status code should be 200")
	assertEqualF(t, transport.getRequests, 1, "Expected exactly one GET request")

	resp, err = internalClient.Post(context.Background(), &url.URL{}, make(map[string]string), make([]byte, 0), 0, defaultTimeProvider)
	assertNilF(t, err, "POST request should succeed")
	assertEqualF(t, resp.StatusCode, 200, "POST response status code should be 200")
	assertEqualF(t, transport.postRequests, 1, "Expected exactly one POST request")

	db.Close()
}
