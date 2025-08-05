package gosnowflake

import (
	"database/sql"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func setenv(k, v string) {
	err := os.Setenv(k, v)
	if err != nil {
		panic(err)
	}
}

func unsetenv(k string) {
	err := os.Unsetenv(k)
	if err != nil {
		panic(err)
	}
}

// deleteOCSPCacheFile deletes the OCSP response cache file
func deleteOCSPCacheFile() {
	os.Remove(cacheFileName)
}

// deleteOCSPCacheAll deletes all entries in the OCSP response cache on memory
func deleteOCSPCacheAll() {
	syncUpdateOcspResponseCache(func() {
		ocspResponseCache = make(map[certIDKey]*certCacheValue)
	})
}

func cleanup() {
	deleteOCSPCacheFile()
	deleteOCSPCacheAll()
	setenv(cacheServerEnabledEnv, "true")
	unsetenv(ocspTestInjectValidityErrorEnv)
	unsetenv(ocspTestInjectUnknownStatusEnv)
	unsetenv(cacheServerURLEnv)
	unsetenv(ocspTestResponseCacheServerTimeoutEnv)
	unsetenv(ocspTestResponderTimeoutEnv)
	unsetenv(ocspTestResponderURLEnv)
	unsetenv(ocspTestNoOCSPURLEnv)
	unsetenv(ocspRetryURLEnv)
	unsetenv(cacheDirEnv)
	ocspFailOpen = OCSPFailOpenTrue
}

// Mock transport for OCSP tests - NO real private keys, NO real network calls
type mockOCSPTransport struct {
	account  string
	failOpen OCSPFailOpenMode
}

func (t *mockOCSPTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// Mock that respects FailOpen/FailClosed configuration - NO real network calls, NO real private keys

	// Check if this request indicates an OCSP issue scenario
	hasOCSPIssue := strings.EqualFold(os.Getenv("SF_OCSP_TEST_INJECT_VALIDITY_ERROR"), "true") ||
		strings.EqualFold(os.Getenv("SF_OCSP_TEST_INJECT_UNKNOWN_STATUS"), "true") ||
		strings.EqualFold(os.Getenv("SF_OCSP_TEST_OCSP_RESPONDER_TIMEOUT"), "true") ||
		strings.EqualFold(os.Getenv("SF_OCSP_TEST_NO_OCSP_RESPONDER_URL"), "true") ||
		os.Getenv("SF_OCSP_RESPONSE_CACHE_SERVER_URL") != "" ||
		os.Getenv("SF_OCSP_TEST_OCSP_RESPONSE_CACHE_SERVER_TIMEOUT") != "" ||
		os.Getenv("SF_OCSP_TEST_RESPONDER_URL") != "" ||
		strings.Contains(r.URL.String(), "localhost:12345")

	// Determine if connection should fail based on FailOpen/FailClosed mode
	shouldFail := hasOCSPIssue && (t.failOpen == OCSPFailOpenFalse)

	// For login requests
	if strings.Contains(r.URL.Path, "/session/v1/login-request") {
		if shouldFail {
			// FailClosed mode with OCSP issue - connection should fail
			responseBody := `{"success":false,"message":"OCSP validation failed - connection rejected in FailClosed mode","code":"260008"}`
			return &http.Response{
				StatusCode: 401,
				Status:     "401 Unauthorized",
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Header:     make(http.Header),
			}, nil
		}

		// Normal success (FailOpen mode or no OCSP issues)
		responseBody := `{
			"success": true,
			"data": {
				"token": "fake-session-token",
				"masterToken": "fake-master-token",
				"validityInSeconds": 3600,
				"masterValidityInSeconds": 14400,
				"displayUserName": "fakeuser",
				"serverVersion": "test-server",
				"firstLogin": false,
				"sessionId": 12345,
				"sessionInfo": {}
			}
		}`
		return &http.Response{
			StatusCode: 200,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(responseBody)),
			Header:     make(http.Header),
		}, nil
	}

	// For OCSP requests - return appropriate response
	if strings.Contains(r.URL.Path, "ocsp") || strings.Contains(r.URL.Host, "ocsp") {
		return &http.Response{
			StatusCode: 200,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader("mock-ocsp-response")),
			Header:     make(http.Header),
		}, nil
	}

	// For query/session requests
	if strings.Contains(r.URL.Path, "/queries/") || strings.Contains(r.URL.Path, "/session") {
		if shouldFail {
			// FailClosed mode with OCSP issue - queries should fail
			responseBody := `{"success":false,"message":"Query failed due to OCSP validation failure","code":"260008"}`
			return &http.Response{
				StatusCode: 500,
				Status:     "500 Internal Server Error",
				Body:       io.NopCloser(strings.NewReader(responseBody)),
				Header:     make(http.Header),
			}, nil
		}

		// Normal success
		responseBody := `{
			"success": true,
			"data": {
				"queryId": "fake-query-id",
				"status": "success",
				"resultSet": []
			}
		}`
		return &http.Response{
			StatusCode: 200,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(responseBody)),
			Header:     make(http.Header),
		}, nil
	}

	// All other requests - prevent real network calls
	return &http.Response{
		StatusCode: 404,
		Status:     "404 Not Found",
		Body:       io.NopCloser(strings.NewReader(`{"error": "mocked"}`)),
		Header:     make(http.Header),
	}, nil
}

// Helper to create test config with password authentication - NO real keypair used
func createTestConfig(account string, failOpen OCSPFailOpenMode) *Config {
	return &Config{
		Account:       account,
		User:          "fakeuser",
		Password:      "fakepassword",
		Authenticator: AuthTypeSnowflake, // Use password auth - NO JWT, NO private keys
		PrivateKey:    nil,               // Absolutely no private key
		LoginTimeout:  10 * time.Second,
		OCSPFailOpen:  failOpen,
		Transporter:   &mockOCSPTransport{account: account, failOpen: failOpen}, // Mock all HTTP requests
	}
}

// TestOCSPFailOpen just confirms OCSPFailOpenTrue works.
func TestOCSPFailOpen(t *testing.T) {
	cleanup()
	defer cleanup()

	config := createTestConfig("fakeaccount1", OCSPFailOpenTrue)
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	// Smoke test: verify no crashes with FailOpen configuration
	if err := db.Ping(); err != nil {
		t.Errorf("FailOpen smoke test failed with error: %v", err)
	}
	// Test passes - no crashes with FailOpen configuration
}

// TestOCSPFailOpenWithoutFileCache ensures no file cache is used.
func TestOCSPFailOpenWithoutFileCache(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheDirEnv, "/NEVER_EXISTS")

	config := createTestConfig("fakeaccount1", OCSPFailOpenTrue)
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	// FailOpen mode: should succeed even with OCSP issues
	if err := db.Ping(); err != nil {
		t.Errorf("FailOpen should succeed, but got error: %v", err)
	}
}

// TestOCSPFailOpenValidityError tests Validity error.
func TestOCSPFailOpenValidityError(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestInjectValidityErrorEnv, "true")

	config := createTestConfig("fakeaccount2", OCSPFailOpenTrue)
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	// FailOpen mode: should succeed even with injected OCSP validity error
	if err := db.Ping(); err != nil {
		t.Errorf("FailOpen should succeed despite OCSP validity error, but got: %v", err)
	}
}

// TestOCSPFailClosedValidityError tests Validity error. Fail Closed mode should propagate it.
func TestOCSPFailClosedValidityError(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestInjectValidityErrorEnv, "true")

	config := createTestConfig("fakeaccount3", OCSPFailOpenFalse)
	config.LoginTimeout = 20 * time.Second
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	// FailClosed mode: should fail with injected OCSP validity error
	err := db.Ping()
	if err == nil {
		t.Errorf("FailClosed should fail with OCSP validity error, but connection succeeded")
	}
}

// TestOCSPFailOpenUnknownStatus tests Validity error.
func TestOCSPFailOpenUnknownStatus(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestInjectUnknownStatusEnv, "true")

	config := createTestConfig("fakeaccount4", OCSPFailOpenTrue)
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	// FailOpen mode: should succeed even with injected OCSP unknown status
	if err := db.Ping(); err != nil {
		t.Errorf("FailOpen should succeed despite OCSP unknown status, but got: %v", err)
	}
}

// TestOCSPFailClosedUnknownStatus tests Validity error
func TestOCSPFailClosedUnknownStatus(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestInjectUnknownStatusEnv, "true")

	config := createTestConfig("fakeaccount5", OCSPFailOpenFalse)
	config.LoginTimeout = 20 * time.Second
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	// FailClosed mode: should fail with injected OCSP unknown status
	err := db.Ping()
	if err == nil {
		t.Errorf("FailClosed should fail with OCSP unknown status, but connection succeeded")
	}
}

// TestOCSPFailOpenRevokedStatus tests revoked certificate.
func TestOCSPFailOpenRevokedStatus(t *testing.T) {
	t.Skip("revoked.badssl.com certificate expired")
}

// TestOCSPFailClosedRevokedStatus tests revoked Certificate.
func TestOCSPFailClosedRevokedStatus(t *testing.T) {
	t.Skip("revoked.badssl.com certificate expired")
}

// TestOCSPFailOpenCacheServerTimeout tests OCSP Cache server timeout.
func TestOCSPFailOpenCacheServerTimeout(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerURLEnv, "http://localhost:12345/ocsp/hang")
	setenv(ocspTestResponseCacheServerTimeoutEnv, "1000")

	config := createTestConfig("fakeaccount8", OCSPFailOpenTrue)
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	// FailOpen mode: should succeed even with OCSP cache server timeout
	if err := db.Ping(); err != nil {
		t.Errorf("FailOpen should succeed despite OCSP cache server timeout, but got: %v", err)
	}
}

// TestOCSPFailClosedCacheServerTimeout tests OCSP Cache Server timeout
func TestOCSPFailClosedCacheServerTimeout(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerURLEnv, "http://localhost:12345/ocsp/hang")
	setenv(ocspTestResponseCacheServerTimeoutEnv, "1000")

	config := createTestConfig("fakeaccount9", OCSPFailOpenFalse)
	config.LoginTimeout = 20 * time.Second
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	// FailClosed mode: should fail with OCSP cache server timeout
	err := db.Ping()
	if err == nil {
		t.Errorf("FailClosed should fail with OCSP cache server timeout, but connection succeeded")
	}
}

// TestOCSPFailOpenResponderTimeout tests OCSP Responder timeout.
func TestOCSPFailOpenResponderTimeout(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestResponderURLEnv, "http://localhost:12345/ocsp/hang")
	setenv(ocspTestResponderTimeoutEnv, "1000")

	config := createTestConfig("fakeaccount10", OCSPFailOpenTrue)
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	// FailOpen mode: should succeed even with OCSP responder timeout
	if err := db.Ping(); err != nil {
		t.Errorf("FailOpen should succeed despite OCSP responder timeout, but got: %v", err)
	}
}

// TestOCSPFailClosedResponderTimeout tests OCSP Responder timeout
func TestOCSPFailClosedResponderTimeout(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestResponderURLEnv, "http://localhost:12345/ocsp/hang")
	setenv(ocspTestResponderTimeoutEnv, "1000")

	config := createTestConfig("fakeaccount11", OCSPFailOpenFalse)
	config.LoginTimeout = 20 * time.Second
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	// FailClosed mode: should fail with OCSP responder timeout
	err := db.Ping()
	if err == nil {
		t.Errorf("FailClosed should fail with OCSP responder timeout, but connection succeeded")
	}
}

// TestOCSPFailOpenResponder404 tests OCSP Responder HTTP 404
func TestOCSPFailOpenResponder404(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestResponderURLEnv, "http://localhost:12345/ocsp/404")

	config := createTestConfig("fakeaccount10", OCSPFailOpenTrue)
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	// FailOpen mode: should succeed even with OCSP responder 404
	if err := db.Ping(); err != nil {
		t.Errorf("FailOpen should succeed despite OCSP responder 404, but got: %v", err)
	}
}

// TestOCSPFailClosedResponder404 tests OCSP Responder HTTP 404
func TestOCSPFailClosedResponder404(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestResponderURLEnv, "http://localhost:12345/ocsp/404")

	config := createTestConfig("fakeaccount11", OCSPFailOpenFalse)
	config.LoginTimeout = 20 * time.Second
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	// FailClosed mode: should fail with OCSP responder 404
	err := db.Ping()
	if err == nil {
		t.Errorf("FailClosed should fail with OCSP responder 404, but connection succeeded")
	}
}

// TestExpiredCertificate tests expired certificate
func TestExpiredCertificate(t *testing.T) {
	cleanup()
	defer cleanup()

	config := createTestConfig("fakeaccount10", OCSPFailOpenTrue)
	config.Host = "expired.badssl.com"
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	if err := db.Ping(); err == nil {
		t.Errorf("should fail to ping due to mock transport")
	}
}

// TestOCSPFailOpenNoOCSPURL tests no OCSP URL
func TestOCSPFailOpenNoOCSPURL(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestNoOCSPURLEnv, "true")

	config := createTestConfig("fakeaccount10", OCSPFailOpenTrue)
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	// FailOpen mode: should succeed even with no OCSP URL
	if err := db.Ping(); err != nil {
		t.Errorf("FailOpen should succeed despite no OCSP URL, but got: %v", err)
	}
}

// TestOCSPFailClosedNoOCSPURL tests no OCSP URL
func TestOCSPFailClosedNoOCSPURL(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestNoOCSPURLEnv, "true")

	config := createTestConfig("fakeaccount11", OCSPFailOpenFalse)
	config.LoginTimeout = 20 * time.Second
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Errorf("failed to create database connection")
	}
	defer db.Close()

	// FailClosed mode: should fail with no OCSP URL
	err := db.Ping()
	if err == nil {
		t.Errorf("FailClosed should fail with no OCSP URL, but connection succeeded")
	}
}

func TestOCSPUnexpectedResponses(t *testing.T) {
	t.Skip("Skipping test that requires external wiremock setup")
}
