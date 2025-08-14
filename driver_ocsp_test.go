package gosnowflake

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"net/http"
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
	syncUpdateOcspResponseCache(func() {
		ocspResponseCache = make(map[certIDKey]*certCacheValue)
	})
}

func cleanup() {
	deleteOCSPCacheFile()
	deleteOCSPCacheAll()
	setenv(cacheServerEnabledEnv, "true")
	unsetenv(cacheServerURLEnv)
	unsetenv(ocspTestResponderURLEnv)
	unsetenv(ocspTestNoOCSPURLEnv)
	unsetenv(ocspRetryURLEnv)
	unsetenv(cacheDirEnv)
	ocspFailOpen = OCSPFailOpenTrue
}

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

func TestOCSPFailOpenCacheServerTimeout(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerURLEnv, fmt.Sprintf("http://localhost:%v/hang", wiremock.port))
	wiremock.registerMappings(t, newWiremockMapping("hang.json"))
	origCacheServerTimeout := OcspCacheServerTimeout
	OcspCacheServerTimeout = time.Second
	defer func() {
		OcspCacheServerTimeout = origCacheServerTimeout
	}()

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

func TestOCSPFailClosedCacheServerTimeout(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerURLEnv, fmt.Sprintf("http://localhost:%v/hang", wiremock.port))
	wiremock.registerMappings(t, newWiremockMapping("hang.json"))
	origCacheServerTimeout := OcspCacheServerTimeout
	OcspCacheServerTimeout = time.Second
	defer func() {
		OcspCacheServerTimeout = origCacheServerTimeout
	}()

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

func TestOCSPFailOpenResponderTimeout(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestResponderURLEnv, fmt.Sprintf("http://localhost:%v/ocsp/hang", wiremock.port))
	wiremock.registerMappings(t, newWiremockMapping("hang.json"))
	origOCSPResponderTimeout := OcspResponderTimeout
	OcspResponderTimeout = 1000
	defer func() {
		OcspResponderTimeout = origOCSPResponderTimeout
	}()

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

func TestOCSPFailClosedResponderTimeout(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestResponderURLEnv, fmt.Sprintf("http://localhost:%v/ocsp/hang", wiremock.port))
	wiremock.registerMappings(t, newWiremockMapping("hang.json"))
	origOCSPResponderTimeout := OcspResponderTimeout
	OcspResponderTimeout = 1000
	defer func() {
		OcspResponderTimeout = origOCSPResponderTimeout
	}()

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

func TestOCSPFailOpenResponder404(t *testing.T) {
	cleanup()
	defer cleanup()

	setenv(cacheServerEnabledEnv, "false")
	setenv(ocspTestResponderURLEnv, "http://localhostŃ:12345/ocsp/404")

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
