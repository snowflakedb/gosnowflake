// Copyright (c) 2019-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"crypto/x509"
	"database/sql"
	"net/url"
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
	ocspResponseCacheLock.Lock()
	defer ocspResponseCacheLock.Unlock()
	ocspResponseCache = make(map[certIDKey]*certCacheValue)
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
}

// TestOCSPFailOpen just confirms OCSPFailOpenTrue works.
func TestOCSPFailOpen(t *testing.T) {
	cleanup()
	defer cleanup()

	config := &Config{
		Account:      "fakeaccount1",
		User:         "fakeuser",
		Password:     "fakepassword",
		LoginTimeout: 10 * time.Second,
		OCSPFailOpen: OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("failed to extract error SnowflakeError: %v", err)
	}
	if isFailToConnectOrAuthErr(driverErr) {
		t.Fatalf("should failed to connect %v", err)
	}
}

func isFailToConnectOrAuthErr(driverErr *SnowflakeError) bool {
	return driverErr.Number != ErrCodeFailedToConnect && driverErr.Number != ErrFailedToAuth
}

// TestOCSPFailOpenWithoutFileCache ensures no file cache is used.
func TestOCSPFailOpenWithoutFileCache(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheDirEnv, "/NEVER_EXISTS")

	config := &Config{
		Account:      "fakeaccount1",
		User:         "fakeuser",
		Password:     "fakepassword",
		LoginTimeout: 10 * time.Second,
		OCSPFailOpen: OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("failed to extract error SnowflakeError: %v", err)
	}
	if isFailToConnectOrAuthErr(driverErr) {
		t.Fatalf("should failed to connect %v", err)
	}
}

// TestOCSPFailOpenValidityError tests Validity error.
func TestOCSPFailOpenValidityError(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestInjectValidityErrorEnv, "true")

	config := &Config{
		Account:      "fakeaccount2",
		User:         "fakeuser",
		Password:     "fakepassword",
		LoginTimeout: 10 * time.Second,
		OCSPFailOpen: OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("failed to extract error SnowflakeError: %v", err)
	}
	if isFailToConnectOrAuthErr(driverErr) {
		t.Fatalf("should failed to connect %v", err)
	}
}

// TestOCSPFailClosedValidityError tests Validity error. Fail Closed mode should propagate it.
func TestOCSPFailClosedValidityError(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestInjectValidityErrorEnv, "true")

	config := &Config{
		Account:      "fakeaccount3",
		User:         "fakeuser",
		Password:     "fakepassword",
		LoginTimeout: 20 * time.Second,
		OCSPFailOpen: OCSPFailOpenFalse,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
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
		Account:      "fakeaccount4",
		User:         "fakeuser",
		Password:     "fakepassword",
		LoginTimeout: 10 * time.Second,
		OCSPFailOpen: OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("failed to extract error SnowflakeError: %v", err)
	}
	if isFailToConnectOrAuthErr(driverErr) {
		t.Fatalf("should failed to connect %v", err)
	}
}

// TestOCSPFailClosedUnknownStatus tests Validity error
func TestOCSPFailClosedUnknownStatus(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestInjectUnknownStatusEnv, "true")

	config := &Config{
		Account:      "fakeaccount5",
		User:         "fakeuser",
		Password:     "fakepassword",
		LoginTimeout: 20 * time.Second,
		OCSPFailOpen: OCSPFailOpenFalse,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
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
		Account:      "fakeaccount6",
		User:         "fakeuser",
		Password:     "fakepassword",
		Host:         "revoked.badssl.com",
		LoginTimeout: 10 * time.Second,
		OCSPFailOpen: OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
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
		Account:      "fakeaccount7",
		User:         "fakeuser",
		Password:     "fakepassword",
		Host:         "revoked.badssl.com",
		LoginTimeout: 20 * time.Second,
		OCSPFailOpen: OCSPFailOpenFalse,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
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
		Account:      "fakeaccount8",
		User:         "fakeuser",
		Password:     "fakepassword",
		LoginTimeout: 10 * time.Second,
		OCSPFailOpen: OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("failed to extract error SnowflakeError: %v", err)
	}
	if isFailToConnectOrAuthErr(driverErr) {
		t.Fatalf("should failed to connect %v", err)
	}
}

// TestOCSPFailClosedCacheServerTimeout tests OCSP Cache Server timeout
func TestOCSPFailClosedCacheServerTimeout(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerURLEnv, "http://localhost:12345/ocsp/hang")
	setenv(ocspTestResponseCacheServerTimeoutEnv, "1000")

	config := &Config{
		Account:      "fakeaccount9",
		User:         "fakeuser",
		Password:     "fakepassword",
		LoginTimeout: 20 * time.Second,
		OCSPFailOpen: OCSPFailOpenFalse,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
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
		if isFailToConnectOrAuthErr(driverErr) {
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
		Account:      "fakeaccount10",
		User:         "fakeuser",
		Password:     "fakepassword",
		LoginTimeout: 10 * time.Second,
		OCSPFailOpen: OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("failed to extract error SnowflakeError: %v", err)
	}
	if isFailToConnectOrAuthErr(driverErr) {
		t.Fatalf("should failed to connect %v", err)
	}
}

// TestOCSPFailClosedResponderTimeout tests OCSP Responder timeout
func TestOCSPFailClosedResponderTimeout(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestResponderURLEnv, "http://localhost:12345/ocsp/hang")
	setenv(ocspTestResponderTimeoutEnv, "1000")

	config := &Config{
		Account:      "fakeaccount11",
		User:         "fakeuser",
		Password:     "fakepassword",
		LoginTimeout: 20 * time.Second,
		OCSPFailOpen: OCSPFailOpenFalse,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
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
		Account:      "fakeaccount10",
		User:         "fakeuser",
		Password:     "fakepassword",
		LoginTimeout: 10 * time.Second,
		OCSPFailOpen: OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("failed to extract error SnowflakeError: %v", err)
	}
	if isFailToConnectOrAuthErr(driverErr) {
		t.Fatalf("should failed to connect %v", err)
	}
}

// TestOCSPFailClosedResponder404 tests OCSP Responder HTTP 404
func TestOCSPFailClosedResponder404(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestResponderURLEnv, "http://localhost:12345/ocsp/404")

	config := &Config{
		Account:      "fakeaccount11",
		User:         "fakeuser",
		Password:     "fakepassword",
		LoginTimeout: 20 * time.Second,
		OCSPFailOpen: OCSPFailOpenFalse,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
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
		Account:      "fakeaccount10",
		User:         "fakeuser",
		Password:     "fakepassword",
		Host:         "expired.badssl.com",
		LoginTimeout: 10 * time.Second,
		OCSPFailOpen: OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
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
		if !strings.Contains(errString, "certificate has expired or is not yet valid") && !strings.Contains(errString, "timeout") {
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
		User:         "fakeuser",
		Password:     "fakepassword",
		Host:         "self-signed.badssl.com",
		LoginTimeout: 10 * time.Second,
		OCSPFailOpen: OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
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
		Account:      "fakeaccount10",
		User:         "fakeuser",
		Password:     "fakepassword",
		LoginTimeout: 10 * time.Second,
		OCSPFailOpen: OCSPFailOpenTrue,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
	}
	if strings.Contains(err.Error(), "HTTP Status: 513. Hanging?") {
		return
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("failed to extract error SnowflakeError: %v", err)
	}
	if isFailToConnectOrAuthErr(driverErr) {
		t.Fatalf("should failed to connect %v", err)
	}
}

// TestOCSPFailClosedNoOCSPURL tests no OCSP URL
func TestOCSPFailClosedNoOCSPURL(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestNoOCSPURLEnv, "true")

	config := &Config{
		Account:      "fakeaccount11",
		User:         "fakeuser",
		Password:     "fakepassword",
		LoginTimeout: 20 * time.Second,
		OCSPFailOpen: OCSPFailOpenFalse,
	}
	var db *sql.DB
	var err error
	var testURL string
	testURL, err = DSN(config)
	if err != nil {
		t.Fatalf("failed to build URL from Config: %v", config)
	}

	if db, err = sql.Open("snowflake", testURL); err != nil {
		t.Fatalf("failed to open db. %v, err: %v", testURL, err)
	}
	defer db.Close()
	if err = db.Ping(); err == nil {
		t.Fatalf("should fail to ping. %v", testURL)
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
