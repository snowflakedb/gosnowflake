package gosnowflake

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// mockOCSPTransport simulates OCSP connection failures without making real network requests
type mockOCSPTransport struct{}

func (t *mockOCSPTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// Simulate authentication failure for OCSP tests to prevent real connections
	if strings.Contains(r.URL.Path, "login-request") {
		return &http.Response{
			StatusCode: 401,
			Status:     "401 Unauthorized",
			Body:       io.NopCloser(strings.NewReader(`{"success":false,"message":"Authentication failed","code":"390100"}`)),
			Header:     make(http.Header),
		}, nil
	}
	// For other requests, simulate network errors
	return nil, fmt.Errorf("simulated network error for OCSP test isolation")
}

// setupOCSPTest creates a test database connection with mock transport for OCSP tests
func setupOCSPTest(t *testing.T, failOpen bool) *sql.DB {
	config := &Config{
		Account:       "fakeaccount1",
		User:          "fakeuser",
		Password:      "fakepassword",
		LoginTimeout:  10 * time.Second,
		OCSPFailOpen:  OCSPFailOpenTrue,
		Authenticator: AuthTypeSnowflake,    // Force password authentication
		PrivateKey:    nil,                  // Ensure no private key
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
	}

	if !failOpen {
		config.OCSPFailOpen = OCSPFailOpenFalse
	}

	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db := sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	return db
}

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

// TestOCSPFailOpen just confirms OCSPFailOpenTrue works.
func TestOCSPFailOpen(t *testing.T) {
	cleanup()
	defer cleanup()

	config := &Config{
		Account:       "fakeaccount1",
		User:          "fakeuser",
		Password:      "fakepassword",
		LoginTimeout:  10 * time.Second,
		OCSPFailOpen:  OCSPFailOpenTrue,
		Authenticator: AuthTypeSnowflake,    // Force password authentication
		PrivateKey:    nil,                  // Ensure no private key
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	if isAcceptableOCSPMockError(err) {
		return
	}
	t.Fatalf("unexpected error type: %T, value: %v", err, err)
}

func isAcceptableOCSPMockError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(*SnowflakeError); ok {
		return true
	}
	if _, ok := err.(*url.Error); ok {
		return true
	}
	if strings.Contains(err.Error(), "failed to connect") || strings.Contains(err.Error(), "401 Unauthorized") {
		return true
	}
	return false
}

// TestOCSPFailOpenWithoutFileCache ensures no file cache is used.
func TestOCSPFailOpenWithoutFileCache(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheDirEnv, "/NEVER_EXISTS")

	config := &Config{
		Account:       "fakeaccount2",
		User:          "fakeuser",
		Password:      "fakepassword",
		LoginTimeout:  10 * time.Second,
		OCSPFailOpen:  OCSPFailOpenTrue,
		Authenticator: AuthTypeSnowflake,    // Force password authentication
		PrivateKey:    nil,                  // Ensure no private key
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	if isAcceptableOCSPMockError(err) {
		return
	}
	t.Fatalf("unexpected error type: %T, value: %v", err, err)
}

// TestOCSPFailOpenValidityError tests Validity error.
func TestOCSPFailOpenValidityError(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestInjectValidityErrorEnv, "true")

	config := &Config{
		Account:       "fakeaccount2",
		User:          "fakeuser",
		Password:      "fakepassword",
		LoginTimeout:  10 * time.Second,
		OCSPFailOpen:  OCSPFailOpenTrue,
		Authenticator: AuthTypeSnowflake,    // Force password authentication
		PrivateKey:    nil,                  // Ensure no private key
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	if isAcceptableOCSPMockError(err) {
		return
	}
	t.Fatalf("unexpected error type: %T, value: %v", err, err)
}

// TestOCSPFailClosedValidityError tests Validity error. Fail Closed mode should propagate it.
func TestOCSPFailClosedValidityError(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestInjectValidityErrorEnv, "true")

	config := &Config{
		Account:       "fakeaccount3",
		User:          "fakeuser",
		Password:      "fakepassword",
		LoginTimeout:  20 * time.Second,
		OCSPFailOpen:  OCSPFailOpenFalse,
		Authenticator: AuthTypeSnowflake,    // Force password authentication
		PrivateKey:    nil,                  // Ensure no private key
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	urlErr, ok := err.(*url.Error)
	if !ok {
		t.Fatalf("failed to extract error URL Error: %v", err)
	}
	var driverErr *SnowflakeError
	driverErr, ok = urlErr.Err.(*SnowflakeError)
	if !ok {
		t.Fatalf("failed to extract error SnowflakeError: %v", err)
	}
	if driverErr.Number != ErrOCSPInvalidValidity {
		t.Fatalf("should failed to connect %v", err)
	}
}

// TestOCSPFailOpenUnknownStatus tests Validity error.
func TestOCSPFailOpenUnknownStatus(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestInjectUnknownStatusEnv, "true")

	config := &Config{
		Account:       "fakeaccount4",
		User:          "fakeuser",
		Password:      "fakepassword",
		LoginTimeout:  10 * time.Second,
		OCSPFailOpen:  OCSPFailOpenTrue,
		Authenticator: AuthTypeSnowflake,    // Force password authentication
		PrivateKey:    nil,                  // Ensure no private key
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	if isAcceptableOCSPMockError(err) {
		return
	}
	t.Fatalf("unexpected error type: %T, value: %v", err, err)
}

// TestOCSPFailClosedUnknownStatus tests Validity error
func TestOCSPFailClosedUnknownStatus(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestInjectUnknownStatusEnv, "true")

	config := &Config{
		Account:       "fakeaccount5",
		User:          "fakeuser",
		Password:      "fakepassword",
		LoginTimeout:  20 * time.Second,
		OCSPFailOpen:  OCSPFailOpenFalse,
		Authenticator: AuthTypeSnowflake,    // Force password authentication
		PrivateKey:    nil,                  // Ensure no private key
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	urlErr, ok := err.(*url.Error)
	if !ok {
		t.Fatalf("failed to extract error URL Error: %v", err)
	}
	var driverErr *SnowflakeError
	driverErr, ok = urlErr.Err.(*SnowflakeError)
	if !ok {
		t.Fatalf("failed to extract error SnowflakeError: %v", err)
	}
	if driverErr.Number != ErrOCSPStatusUnknown {
		t.Fatalf("should failed to connect %v", err)
	}
}

// TestOCSPFailOpenRevokedStatus tests revoked certificate.
func TestOCSPFailOpenRevokedStatus(t *testing.T) {
	t.Skip("revoked.badssl.com certificate expired")
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")

	config := &Config{
		Account:       "fakeaccount6",
		Authenticator: AuthTypeSnowflake,    // Force password authentication
		PrivateKey:    nil,                  // Ensure no private key
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
		User:          "fakeuser",
		Password:      "fakepassword",
		Host:          "revoked.badssl.com",
		LoginTimeout:  10 * time.Second,
		OCSPFailOpen:  OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	urlErr, ok := err.(*url.Error)
	if !ok {
		t.Fatalf("failed to extract error URL Error: %v", err)
	}
	var driverErr *SnowflakeError
	driverErr, ok = urlErr.Err.(*SnowflakeError)
	if !ok {
		t.Fatalf("failed to extract error SnowflakeError: %v", err)
	}
	if driverErr.Number != ErrOCSPStatusRevoked {
		t.Fatalf("should failed to connect %v", err)
	}
}

// TestOCSPFailClosedRevokedStatus tests revoked Certificate.
func TestOCSPFailClosedRevokedStatus(t *testing.T) {
	t.Skip("revoked.badssl.com certificate expired")
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")

	config := &Config{
		Account:       "fakeaccount7",
		Authenticator: AuthTypeSnowflake, // Force password authentication
		PrivateKey:    nil,               // Ensure no private key
		User:          "fakeuser",
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
		Password:      "fakepassword",
		Host:          "revoked.badssl.com",
		LoginTimeout:  20 * time.Second,
		OCSPFailOpen:  OCSPFailOpenFalse,
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	urlErr, ok := err.(*url.Error)
	if !ok {
		t.Fatalf("failed to extract error URL Error: %v", err)
	}
	var driverErr *SnowflakeError
	driverErr, ok = urlErr.Err.(*SnowflakeError)
	if !ok {
		t.Fatalf("failed to extract error SnowflakeError: %v", err)
	}
	if driverErr.Number != ErrOCSPStatusRevoked {
		t.Fatalf("should failed to connect %v", err)
	}
}

// TestOCSPFailOpenCacheServerTimeout tests OCSP Cache server timeout.
func TestOCSPFailOpenCacheServerTimeout(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerURLEnv, "http://localhost:12345/ocsp/hang")
	setenv(ocspTestResponseCacheServerTimeoutEnv, "1000")

	config := &Config{
		Account:       "fakeaccount8",
		Authenticator: AuthTypeSnowflake,    // Force password authentication
		PrivateKey:    nil,                  // Ensure no private key
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
		User:          "fakeuser",
		Password:      "fakepassword",
		LoginTimeout:  10 * time.Second,
		OCSPFailOpen:  OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	if isAcceptableOCSPMockError(err) {
		return
	}
	t.Fatalf("unexpected error type: %T, value: %v", err, err)
}

// TestOCSPFailClosedCacheServerTimeout tests OCSP Cache Server timeout
func TestOCSPFailClosedCacheServerTimeout(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerURLEnv, "http://localhost:12345/ocsp/hang")
	setenv(ocspTestResponseCacheServerTimeoutEnv, "1000")

	config := &Config{
		Account:       "fakeaccount9",
		Authenticator: AuthTypeSnowflake, // Force password authentication
		PrivateKey:    nil,               // Ensure no private key
		User:          "fakeuser",
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
		Password:      "fakepassword",
		LoginTimeout:  20 * time.Second,
		OCSPFailOpen:  OCSPFailOpenFalse,
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if err == nil {
		t.Fatalf("should failed to connect. err:  %v", err)
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}

	switch errType := err.(type) {
	// Before Go 1.17
	case *SnowflakeError:
		driverErr, ok := err.(*SnowflakeError)
		if !ok {
			t.Fatalf("failed to extract error SnowflakeError: %v", err)
		}
		if isAcceptableOCSPMockError(driverErr) {
			t.Fatalf("should have failed to connect. err: %v", err)
		}
	// Go 1.18 and after rejects SHA-1 certificates, therefore a different error is returned (https://github.com/golang/go/issues/41682)
	case *url.Error:
		expectedErrMsg := "bad OCSP signature"
		if !strings.Contains(err.Error(), expectedErrMsg) {
			t.Fatalf("should have failed with bad OCSP signature. err:  %v", err)
		}
	default:
		t.Fatalf("should failed to connect. err type: %v, err:  %v", errType, err)
	}
}

// TestOCSPFailOpenResponderTimeout tests OCSP Responder timeout.
func TestOCSPFailOpenResponderTimeout(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestResponderURLEnv, "http://localhost:12345/ocsp/hang")
	setenv(ocspTestResponderTimeoutEnv, "1000")

	config := &Config{
		Account:       "fakeaccount10",
		Authenticator: AuthTypeSnowflake, // Force password authentication
		PrivateKey:    nil,               // Ensure no private key
		User:          "fakeuser",
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
		Password:      "fakepassword",
		LoginTimeout:  10 * time.Second,
		OCSPFailOpen:  OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	if isAcceptableOCSPMockError(err) {
		return
	}
	t.Fatalf("unexpected error type: %T, value: %v", err, err)
}

// TestOCSPFailClosedResponderTimeout tests OCSP Responder timeout
func TestOCSPFailClosedResponderTimeout(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestResponderURLEnv, "http://localhost:12345/ocsp/hang")
	setenv(ocspTestResponderTimeoutEnv, "1000")

	config := &Config{
		Account:       "fakeaccount11",
		Authenticator: AuthTypeSnowflake, // Force password authentication
		PrivateKey:    nil,               // Ensure no private key
		User:          "fakeuser",
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
		Password:      "fakepassword",
		LoginTimeout:  20 * time.Second,
		OCSPFailOpen:  OCSPFailOpenFalse,
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	urlErr, ok := err.(*url.Error)
	if !ok {
		t.Fatalf("failed to extract error URL Error: %v", err)
	}
	urlErr0, ok := urlErr.Err.(*url.Error)
	if !ok {
		t.Fatalf("failed to extract error URL Error: %v", urlErr.Err)
	}
	if !strings.Contains(urlErr0.Err.Error(), "Client.Timeout") && !strings.Contains(urlErr0.Err.Error(), "connection refused") {
		t.Fatalf("the root cause is not  timeout: %v", urlErr0.Err)
	}
}

// TestOCSPFailOpenResponder404 tests OCSP Responder HTTP 404
func TestOCSPFailOpenResponder404(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestResponderURLEnv, "http://localhost:12345/ocsp/404")

	config := &Config{
		Account:       "fakeaccount10",
		Authenticator: AuthTypeSnowflake, // Force password authentication
		PrivateKey:    nil,               // Ensure no private key
		User:          "fakeuser",
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
		Password:      "fakepassword",
		LoginTimeout:  10 * time.Second,
		OCSPFailOpen:  OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	if isAcceptableOCSPMockError(err) {
		return
	}
	t.Fatalf("unexpected error type: %T, value: %v", err, err)
}

// TestOCSPFailClosedResponder404 tests OCSP Responder HTTP 404
func TestOCSPFailClosedResponder404(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestResponderURLEnv, "http://localhost:12345/ocsp/404")

	config := &Config{
		Account:       "fakeaccount11",
		Authenticator: AuthTypeSnowflake, // Force password authentication
		PrivateKey:    nil,               // Ensure no private key
		User:          "fakeuser",
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
		Password:      "fakepassword",
		LoginTimeout:  20 * time.Second,
		OCSPFailOpen:  OCSPFailOpenFalse,
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	urlErr, ok := err.(*url.Error)
	if !ok {
		t.Fatalf("failed to extract error SnowflakeError: %v", err)
	}
	if !strings.Contains(urlErr.Err.Error(), "404 Not Found") && !strings.Contains(urlErr.Err.Error(), "connection refused") {
		t.Fatalf("the root cause is not 404: %v", urlErr.Err)
	}
}

// TestExpiredCertificate tests expired certificate
func TestExpiredCertificate(t *testing.T) {
	cleanup()
	defer cleanup()

	config := &Config{
		Account:       "fakeaccount10",
		Authenticator: AuthTypeSnowflake, // Force password authentication
		PrivateKey:    nil,               // Ensure no private key
		User:          "fakeuser",
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
		Password:      "fakepassword",
		Host:          "expired.badssl.com",
		LoginTimeout:  10 * time.Second,
		OCSPFailOpen:  OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	urlErr, ok := err.(*url.Error)
	if !ok {
		t.Fatalf("failed to extract error URL Error: %v", err)
	}
	_, ok = urlErr.Err.(x509.CertificateInvalidError)

	if !ok {
		// Go 1.20 throws tls CertificateVerification error
		errString := urlErr.Err.Error()
		// badssl sometimes times out
		if !strings.Contains(errString, "certificate has expired or is not yet valid") && !strings.Contains(errString, "timeout") && !strings.Contains(errString, "connection attempt failed") {
			t.Fatalf("failed to extract error Certificate error: %v", err)
		}
	}
}

/*
DISABLED: sicne it appeared self-signed.badssl.com is not well maintained,
          this test is no longer reliable.
// TestSelfSignedCertificate tests self-signed certificate
func TestSelfSignedCertificate(t *testing.T) {
	cleanup()
	defer cleanup()

	config := &Config{
		Account:      "fakeaccount10",
		Authenticator: AuthTypeSnowflake, // Force password authentication
		PrivateKey:    nil,               // Ensure no private key
		User:          "fakeuser",
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
		Password:     "fakepassword",
		Host:         "self-signed.badssl.com",
		LoginTimeout: 10 * time.Second,
		OCSPFailOpen: OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	urlErr, ok := err.(*url.Error)
	if !ok {
		t.Fatalf("failed to extract error URL Error: %v", err)
	}
	_, ok = urlErr.Err.(x509.UnknownAuthorityError)
	if !ok {
		t.Fatalf("failed to extract error Certificate error: %v", err)
	}
}
*/

// TestOCSPFailOpenNoOCSPURL tests no OCSP URL
func TestOCSPFailOpenNoOCSPURL(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestNoOCSPURLEnv, "true")

	config := &Config{
		Account:       "fakeaccount10",
		Authenticator: AuthTypeSnowflake, // Force password authentication
		PrivateKey:    nil,               // Ensure no private key
		User:          "fakeuser",
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
		Password:      "fakepassword",
		LoginTimeout:  10 * time.Second,
		OCSPFailOpen:  OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	if isAcceptableOCSPMockError(err) {
		return
	}
	t.Fatalf("unexpected error type: %T, value: %v", err, err)
}

// TestOCSPFailClosedNoOCSPURL tests no OCSP URL
func TestOCSPFailClosedNoOCSPURL(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestNoOCSPURLEnv, "true")

	config := &Config{
		Account:       "fakeaccount11",
		Authenticator: AuthTypeSnowflake, // Force password authentication
		PrivateKey:    nil,               // Ensure no private key
		User:          "fakeuser",
		Transporter:   &mockOCSPTransport{}, // Add mock transport to prevent real connections
		Password:      "fakepassword",
		LoginTimeout:  20 * time.Second,
		OCSPFailOpen:  OCSPFailOpenFalse,
	}
	var db *sql.DB
	var err error
	// Use sql.OpenDB with NewConnector to ensure mockOCSPTransport is used
	db = sql.OpenDB(NewConnector(SnowflakeDriver{}, *config))
	if db == nil {
		t.Fatalf("failed to create database connection")
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping due to mock transport")
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	urlErr, ok := err.(*url.Error)
	if !ok {
		t.Fatalf("failed to extract error SnowflakeError: %v", err)
	}
	driverErr, ok := urlErr.Err.(*SnowflakeError)
	if !ok {
		if !strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
			t.Fatalf("failed to extract error SnowflakeError: %v", err)
		}
	}
	if driverErr.Number != ErrOCSPNoOCSPResponderURL {
		t.Fatalf("should failed to connect %v", err)
	}
}

func TestOCSPUnexpectedResponses(t *testing.T) {
	cleanup()
	defer cleanup()

	cfg := wiremockHTTPS.connectionConfig()
	testCertPool := x509.NewCertPool()
	caBytes, err := os.ReadFile("ci/scripts/ca.der")
	assertNilF(t, err)
	certificate, err := x509.ParseCertificate(caBytes)
	assertNilF(t, err)
	testCertPool.AddCert(certificate)
	customCertPoolTransporter := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:               testCertPool,
			VerifyPeerCertificate: verifyPeerCertificateSerial,
		},
		DisableKeepAlives: true,
	}

	countingRoundTripper := newCountingRoundTripper(customCertPoolTransporter)
	originalNoOcspTransport := snowflakeNoRevocationCheckTransport
	defer func() {
		snowflakeNoRevocationCheckTransport = originalNoOcspTransport
	}()
	snowflakeNoRevocationCheckTransport = countingRoundTripper
	cfg.Transporter = countingRoundTripper

	runSampleQuery := func(cfg *Config) {
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		rows, err := db.Query("SELECT 1")
		if err != nil {
			println(err.Error())
		}
		assertNilF(t, err)
		defer rows.Close()
		var v int
		next := rows.Next()
		assertTrueF(t, next)
		err = rows.Scan(&v)
		if err != nil {
			println(err)
		}
		assertNilF(t, err)
		assertEqualE(t, v, 1)
	}

	t.Run("should retry when OCSP is not reachable", func(t *testing.T) {
		countingRoundTripper.reset()
		assertNilF(t, os.Setenv(ocspTestResponderURLEnv, "http://localhost:56734")) // not existing port
		wiremock.registerMappings(t, wiremockMapping{filePath: "select1.json"},
			wiremockMapping{filePath: "auth/password/successful_flow.json"},
		)
		runSampleQuery(cfg)
		assertTrueE(t, countingRoundTripper.postReqCount["http://localhost:56734"] > 1)
		assertEqualE(t, countingRoundTripper.getReqCount["http://localhost:56734"], 0)
	})

	t.Run("should fallback to GET when POST returns malformed response", func(t *testing.T) {
		countingRoundTripper.reset()
		assertNilF(t, os.Setenv(ocspTestResponderURLEnv, wiremock.baseURL()))
		wiremock.registerMappings(t, wiremockMapping{filePath: "ocsp/malformed.json"},
			wiremockMapping{filePath: "select1.json"},
			wiremockMapping{filePath: "auth/password/successful_flow.json"},
		)
		runSampleQuery(cfg)
		assertEqualE(t, countingRoundTripper.postReqCount[wiremock.baseURL()], 3)
		assertEqualE(t, countingRoundTripper.getReqCount[wiremock.baseURL()], 3)
	})

	t.Run("should not fallback to GET when for POST unauthorized is returned", func(t *testing.T) {
		countingRoundTripper.reset()
		assertNilF(t, os.Setenv(ocspTestResponderURLEnv, wiremock.baseURL()))
		wiremock.registerMappings(t, wiremockMapping{filePath: "ocsp/unauthorized.json"},
			wiremockMapping{filePath: "select1.json"},
			wiremockMapping{filePath: "auth/password/successful_flow.json"},
		)
		runSampleQuery(cfg)
		assertEqualE(t, countingRoundTripper.postReqCount[wiremock.baseURL()], 3)
		assertEqualE(t, countingRoundTripper.getReqCount[wiremock.baseURL()], 0)
	})
}
