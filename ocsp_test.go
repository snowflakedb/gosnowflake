package gosnowflake

import (
	"bytes"
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"golang.org/x/crypto/ocsp"
)

// resetOCSPCaches resets the in-memory OCSP caches to empty maps without
// touching the on-disk cache or checking ocspCacheServerEnabled.
func resetOCSPCaches() {
	ocspResponseCacheLock.Lock()
	ocspResponseCache = make(map[certIDKey]*certCacheValue)
	ocspResponseCacheLock.Unlock()
	ocspParsedRespCacheLock.Lock()
	ocspParsedRespCache = make(map[parsedOcspRespKey]*ocspStatus)
	ocspParsedRespCacheLock.Unlock()
}

// resetOCSPCachesForTest resets the in-memory OCSP caches and registers
// t.Cleanup to reset them again at test end, making the test hermetic with
// respect to global OCSP cache state.
func resetOCSPCachesForTest(t *testing.T) {
	t.Helper()
	resetOCSPCaches()
	t.Cleanup(resetOCSPCaches)
}

func TestOCSP(t *testing.T) {
	cacheServerEnabled := []string{
		"true",
		"false",
	}
	targetURL := []string{
		"https://sfctest0.snowflakecomputing.com/",
		"https://s3-us-west-2.amazonaws.com/sfc-snowsql-updates/?prefix=1.1/windows_x86_64",
		"https://sfcdev2.blob.core.windows.net/",
	}

	ocspTransport, err := newTransportFactory(&Config{}, nil).createOCSPTransport(defaultTransportConfigs.forTransportType(transportTypeSnowflake))
	assertNilF(t, err)

	transports := []http.RoundTripper{
		createTestNoRevocationTransport(),
		ocspTransport,
	}

	for _, enabled := range cacheServerEnabled {
		for _, tgt := range targetURL {
			_ = os.Setenv(cacheServerEnabledEnv, enabled)
			_ = os.Remove(cacheFileName) // clear cache file
			syncUpdateOcspResponseCache(func() {
				ocspResponseCache = make(map[certIDKey]*certCacheValue)
			})
			for _, tr := range transports {
				t.Run(fmt.Sprintf("%v_%v", tgt, enabled), func(t *testing.T) {
					c := &http.Client{
						Transport: tr,
						Timeout:   30 * time.Second,
					}
					req, err := http.NewRequest("GET", tgt, bytes.NewReader(nil))
					if err != nil {
						t.Fatalf("fail to create a request. err: %v", err)
					}
					res, err := c.Do(req)
					if err != nil {
						t.Fatalf("failed to GET contents. err: %v", err)
					}
					defer res.Body.Close()
					_, err = io.ReadAll(res.Body)
					if err != nil {
						t.Fatalf("failed to read content body for %v", tgt)
					}
				})
			}
		}
	}
	_ = os.Unsetenv(cacheServerEnabledEnv)
}

type tcValidityRange struct {
	thisTime time.Time
	nextTime time.Time
	ret      bool
}

func TestUnitIsInValidityRange(t *testing.T) {
	currentTime := time.Now()
	testcases := []tcValidityRange{
		{
			// basic tests
			thisTime: currentTime.Add(-100 * time.Second),
			nextTime: currentTime.Add(maxClockSkew),
			ret:      true,
		},
		{
			// on the border
			thisTime: currentTime.Add(maxClockSkew),
			nextTime: currentTime.Add(maxClockSkew),
			ret:      true,
		},
		{
			// 1 earlier late
			thisTime: currentTime.Add(maxClockSkew + 1*time.Second),
			nextTime: currentTime.Add(maxClockSkew),
			ret:      false,
		},
		{
			// on the border
			thisTime: currentTime.Add(-maxClockSkew),
			nextTime: currentTime.Add(-maxClockSkew),
			ret:      true,
		},
		{
			// around the border
			thisTime: currentTime.Add(-24*time.Hour - 40*time.Second),
			nextTime: currentTime.Add(-24*time.Hour/time.Duration(100) - 40*time.Second),
			ret:      false,
		},
		{
			// on the border
			thisTime: currentTime.Add(-48*time.Hour - 29*time.Minute),
			nextTime: currentTime.Add(-48 * time.Hour / time.Duration(100)),
			ret:      true,
		},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%v_%v", tc.thisTime, tc.nextTime), func(t *testing.T) {
			if tc.ret != isInValidityRange(currentTime, tc.thisTime, tc.nextTime) {
				t.Fatalf("failed to check validity. should be: %v, currentTime: %v, thisTime: %v, nextTime: %v", tc.ret, currentTime, tc.thisTime, tc.nextTime)
			}
		})
	}
}

func TestUnitEncodeCertIDGood(t *testing.T) {
	targetURLs := []string{
		"faketestaccount.snowflakecomputing.com:443",
		"s3-us-west-2.amazonaws.com:443",
		"sfcdev2.blob.core.windows.net:443",
	}
	for _, tt := range targetURLs {
		t.Run(tt, func(t *testing.T) {
			chainedCerts := getCert(tt)
			for i := 0; i < len(chainedCerts)-1; i++ {
				subject := chainedCerts[i]
				issuer := chainedCerts[i+1]
				ocspServers := subject.OCSPServer
				if len(ocspServers) == 0 {
					t.Fatalf("no OCSP server is found. cert: %v", subject.Subject)
				}
				ocspReq, err := ocsp.CreateRequest(subject, issuer, &ocsp.RequestOptions{})
				if err != nil {
					t.Fatalf("failed to create OCSP request. err: %v", err)
				}
				var ost *ocspStatus
				_, ost = extractCertIDKeyFromRequest(ocspReq)
				if ost.err != nil {
					t.Fatalf("failed to extract cert ID from the OCSP request. err: %v", ost.err)
				}
				// better hash. Not sure if the actual OCSP server accepts this, though.
				ocspReq, err = ocsp.CreateRequest(subject, issuer, &ocsp.RequestOptions{Hash: crypto.SHA512})
				if err != nil {
					t.Fatalf("failed to create OCSP request. err: %v", err)
				}
				_, ost = extractCertIDKeyFromRequest(ocspReq)
				if ost.err != nil {
					t.Fatalf("failed to extract cert ID from the OCSP request. err: %v", ost.err)
				}
				// tweaked request binary
				ocspReq, err = ocsp.CreateRequest(subject, issuer, &ocsp.RequestOptions{Hash: crypto.SHA512})
				if err != nil {
					t.Fatalf("failed to create OCSP request. err: %v", err)
				}
				ocspReq[10] = 0 // random change
				_, ost = extractCertIDKeyFromRequest(ocspReq)
				if ost.err == nil {
					t.Fatal("should have failed")
				}
			}
		})
	}
}

func TestUnitCheckOCSPResponseCache(t *testing.T) {
	resetOCSPCachesForTest(t)
	ocspCacheServerEnabled = true
	ov := newOcspValidator(&Config{OCSPFailOpen: OCSPFailOpenTrue})
	dummyKey0 := certIDKey{
		HashAlgorithm: crypto.SHA1,
		NameHash:      "dummy0",
		IssuerKeyHash: "dummy0",
		SerialNumber:  "dummy0",
	}
	dummyKey := certIDKey{
		HashAlgorithm: crypto.SHA1,
		NameHash:      "dummy1",
		IssuerKeyHash: "dummy1",
		SerialNumber:  "dummy1",
	}
	b64Key := base64.StdEncoding.EncodeToString([]byte("DUMMY_VALUE"))
	currentTime := float64(time.Now().UTC().Unix())
	syncUpdateOcspResponseCache(func() {
		ocspResponseCache[dummyKey0] = &certCacheValue{currentTime, b64Key}
	})
	subject := &x509.Certificate{}
	issuer := &x509.Certificate{}
	ost := ov.checkOCSPResponseCache(&dummyKey, subject, issuer)
	if ost.code != ocspMissedCache {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspMissedCache, ost.code)
	}
	// old timestamp
	syncUpdateOcspResponseCache(func() {
		ocspResponseCache[dummyKey] = &certCacheValue{float64(1395054952), b64Key}
	})
	ost = ov.checkOCSPResponseCache(&dummyKey, subject, issuer)
	if ost.code != ocspCacheExpired {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspCacheExpired, ost.code)
	}
	// future timestamp
	syncUpdateOcspResponseCache(func() {
		ocspResponseCache[dummyKey] = &certCacheValue{float64(1805054952), b64Key}
	})
	ost = ov.checkOCSPResponseCache(&dummyKey, subject, issuer)
	if ost.code != ocspFailedParseResponse {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspFailedDecodeResponse, ost.code)
	}
	// actual OCSP but it fails to parse, because an invalid issuer certificate is given.
	actualOcspResponse := "MIIB0woBAKCCAcwwggHIBgkrBgEFBQcwAQEEggG5MIIBtTCBnqIWBBSxPsNpA/i/RwHUmCYaCALvY2QrwxgPMjAxNz" + // pragma: allowlist secret
		"A1MTYyMjAwMDBaMHMwcTBJMAkGBSsOAwIaBQAEFN+qEuMosQlBk+KfQoLOR0BClVijBBSxPsNpA/i/RwHUmCYaCALvY2QrwwIQBOHnp" + // pragma: allowlist secret
		"Nxc8vNtwCtCuF0Vn4AAGA8yMDE3MDUxNjIyMDAwMFqgERgPMjAxNzA1MjMyMjAwMDBaMA0GCSqGSIb3DQEBCwUAA4IBAQCuRGwqQsKy" + // pragma: allowlist secret
		"IAAGHgezTfG0PzMYgGD/XRDhU+2i08WTJ4Zs40Lu88cBeRXWF3iiJSpiX3/OLgfI7iXmHX9/sm2SmeNWc0Kb39bk5Lw1jwezf8hcI9+" + // pragma: allowlist secret
		"mZHt60vhUgtgZk21SsRlTZ+S4VXwtDqB1Nhv6cnSnfrL2A9qJDZS2ltPNOwebWJnznDAs2dg+KxmT2yBXpHM1kb0EOolWvNgORbgIgB" + // pragma: allowlist secret
		"koRzw/UU7zKsqiTB0ZN/rgJp+MocTdqQSGKvbZyR8d4u8eNQqi1x4Pk3yO/pftANFaJKGB+JPgKS3PQAqJaXcipNcEfqtl7y4PO6kqA" + // pragma: allowlist secret
		"Jb4xI/OTXIrRA5TsT4cCioE"
	// issuer is not a true issuer certificate
	syncUpdateOcspResponseCache(func() {
		ocspResponseCache[dummyKey] = &certCacheValue{float64(currentTime - 1000), actualOcspResponse}
	})
	ost = ov.checkOCSPResponseCache(&dummyKey, subject, issuer)
	if ost.code != ocspFailedParseResponse {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspFailedParseResponse, ost.code)
	}
	// invalid validity
	syncUpdateOcspResponseCache(func() {
		ocspResponseCache[dummyKey] = &certCacheValue{float64(currentTime - 1000), actualOcspResponse}
	})
	ost = ov.checkOCSPResponseCache(&dummyKey, subject, nil)
	if ost.code != ocspInvalidValidity {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspInvalidValidity, ost.code)
	}
}

// TestExtractOCSPCacheResponseValueVerifiesSignatureAndSerial is a regression
// test for SNOW-3649697. A cached OCSP response must only be trusted as a
// Good/Revoked verdict after (a) its signature is verified against the issuer
// and (b) it is confirmed to correspond to the certificate being validated.
// It also asserts that the cache-load path (nil subject/issuer) never populates
// the parsed-response memo, so an unverified response is not memoized as Good
// and served to the issuer-aware handshake lookup.
func TestExtractOCSPCacheResponseValueVerifiesSignatureAndSerial(t *testing.T) {
	ocspCacheServerEnabled = true

	// Issuing CA and the leaf certificate it signs.
	caKey, caCert := createCa(t, nil, nil, "Test Root CA", 0)
	_, leafCert := createLeafCert(t, caCert, caKey, 0)
	// A second, unrelated CA (different key) used to produce responses that
	// must be rejected.
	otherCAKey, otherCACert := createCa(t, nil, nil, "Other CA", 0)

	now := time.Now()
	cacheValueFor := func(respDER []byte) *certCacheValue {
		return &certCacheValue{
			ts:             float64(now.UTC().Unix()),
			ocspRespBase64: base64.StdEncoding.EncodeToString(respDER),
		}
	}
	goodTemplate := func(serial *big.Int) ocsp.Response {
		return ocsp.Response{
			Status:       ocsp.Good,
			SerialNumber: serial,
			ThisUpdate:   now.Add(-time.Hour),
			NextUpdate:   now.Add(time.Hour),
		}
	}
	certID := &certIDKey{
		HashAlgorithm: crypto.SHA1,
		NameHash:      "victim",
		IssuerKeyHash: "victim",
		SerialNumber:  leafCert.SerialNumber.String(),
	}
	parsedRespCacheLen := func() int {
		ocspParsedRespCacheLock.Lock()
		defer ocspParsedRespCacheLock.Unlock()
		return len(ocspParsedRespCache)
	}

	t.Run("Good signed by an unrelated CA is rejected and not memoized", func(t *testing.T) {
		resetOCSPCachesForTest(t)
		mismatched, err := ocsp.CreateResponse(otherCACert, otherCACert, goodTemplate(leafCert.SerialNumber), otherCAKey)
		assertNilF(t, err)
		value := cacheValueFor(mismatched)

		// Cache-load path (nil subject/issuer): an unverified verdict must not
		// be written to the memo; only the verified handshake path may do so.
		extractOCSPCacheResponseValueWithoutSubject(certID, value)
		assertEqualE(t, parsedRespCacheLen(), 0)

		// Handshake path verifies against the issuing CA; the unrelated signature fails.
		status := extractOCSPCacheResponseValue(certID, value, leafCert, caCert)
		assertEqualE(t, status.code, ocspFailedParseResponse)
		assertEqualE(t, parsedRespCacheLen(), 0)
	})

	t.Run("legit Good for a different serial is rejected", func(t *testing.T) {
		resetOCSPCachesForTest(t)
		_, otherLeaf := createLeafCert(t, caCert, caKey, 0)
		// Validly signed by the real CA, but about a different certificate.
		respForOther, err := ocsp.CreateResponse(caCert, caCert, goodTemplate(otherLeaf.SerialNumber), caKey)
		assertNilF(t, err)
		value := cacheValueFor(respForOther)

		status := extractOCSPCacheResponseValue(certID, value, leafCert, caCert)
		assertEqualE(t, status.code, ocspFailedParseResponse)
	})

	t.Run("genuine Good for the right cert and CA is accepted and memoized", func(t *testing.T) {
		resetOCSPCachesForTest(t)
		good, err := ocsp.CreateResponse(caCert, caCert, goodTemplate(leafCert.SerialNumber), caKey)
		assertNilF(t, err)
		value := cacheValueFor(good)

		status := extractOCSPCacheResponseValue(certID, value, leafCert, caCert)
		assertEqualE(t, status.code, ocspStatusGood)
		// A verified verdict is memoized for reuse on subsequent lookups.
		assertEqualE(t, parsedRespCacheLen(), 1)
	})
}

func TestOcspCacheClearer(t *testing.T) {
	resetOCSPCachesForTest(t)
	origValue := os.Getenv(ocspResponseCacheClearingIntervalInSecondsEnv)
	defer func() {
		StopOCSPCacheClearer()
		os.Setenv(ocspResponseCacheClearingIntervalInSecondsEnv, origValue)
		resetOCSPCaches()
		StartOCSPCacheClearer()
	}()
	syncUpdateOcspResponseCache(func() {
		ocspResponseCache[certIDKey{}] = nil
	})
	func() {
		ocspParsedRespCacheLock.Lock()
		defer ocspParsedRespCacheLock.Unlock()
		ocspParsedRespCache[parsedOcspRespKey{}] = nil
	}()
	StopOCSPCacheClearer()
	os.Setenv(ocspResponseCacheClearingIntervalInSecondsEnv, "1")
	StartOCSPCacheClearer()
	time.Sleep(2 * time.Second)
	syncUpdateOcspResponseCache(func() {
		assertEqualE(t, len(ocspResponseCache), 0)
	})
	func() {
		ocspParsedRespCacheLock.Lock()
		defer ocspParsedRespCacheLock.Unlock()
		assertEqualE(t, len(ocspParsedRespCache), 0)
	}()
}

func TestUnitValidateOCSP(t *testing.T) {
	ocspRes := &ocsp.Response{
		ThisUpdate: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		NextUpdate: time.Date(2020, 1, 5, 0, 0, 0, 0, time.UTC),
	}
	ost := validateOCSP(ocspRes)
	if ost.code != ocspInvalidValidity {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspInvalidValidity, ost.code)
	}
	currentTime := time.Now()
	ocspRes.ThisUpdate = currentTime.Add(-2 * time.Hour)
	ocspRes.NextUpdate = currentTime.Add(2 * time.Hour)
	ocspRes.Status = ocsp.Revoked
	ost = validateOCSP(ocspRes)
	if ost.code != ocspStatusRevoked {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspStatusRevoked, ost.code)
	}
	ocspRes.Status = ocsp.Good
	ost = validateOCSP(ocspRes)
	if ost.code != ocspStatusGood {
		t.Fatalf("should have success. expected: %v, got: %v", ocspStatusGood, ost.code)
	}
	ocspRes.Status = ocsp.Unknown
	ost = validateOCSP(ocspRes)
	if ost.code != ocspStatusUnknown {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspStatusUnknown, ost.code)
	}
	ocspRes.Status = ocsp.ServerFailed
	ost = validateOCSP(ocspRes)
	if ost.code != ocspStatusOthers {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspStatusOthers, ost.code)
	}
}

func TestUnitEncodeCertID(t *testing.T) {
	var st *ocspStatus
	_, st = extractCertIDKeyFromRequest([]byte{0x1, 0x2})
	if st.code != ocspFailedDecomposeRequest {
		t.Fatalf("failed to get OCSP status. expected: %v, got: %v", ocspFailedDecomposeRequest, st.code)
	}
}

func getCert(addr string) []*x509.Certificate {
	tcpConn, err := net.DialTimeout("tcp", addr, 40*time.Second)
	if err != nil {
		panic(err)
	}
	defer tcpConn.Close()

	err = tcpConn.SetDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		panic(err)
	}
	config := tls.Config{InsecureSkipVerify: true, ServerName: addr}

	conn := tls.Client(tcpConn, &config)
	defer conn.Close()

	err = conn.Handshake()
	if err != nil {
		panic(err)
	}

	state := conn.ConnectionState()

	return state.PeerCertificates
}

func TestOCSPRetry(t *testing.T) {
	ov := newOcspValidator(&Config{OCSPFailOpen: OCSPFailOpenTrue})
	certs := getCert("s3-us-west-2.amazonaws.com:443")
	dummyOCSPHost := &url.URL{
		Scheme: "https",
		Host:   "dummyOCSPHost",
	}
	client := &fakeHTTPClient{
		cnt:     3,
		success: true,
		body:    []byte{1, 2, 3},
		t:       t,
	}
	res, b, st := ov.retryOCSP(
		context.Background(),
		client, emptyRequest,
		dummyOCSPHost,
		make(map[string]string), []byte{0}, certs[0], certs[len(certs)-1], 10*time.Second)
	if st.err == nil {
		fmt.Printf("should fail: %v, %v, %v\n", res, b, st)
	}
	client = &fakeHTTPClient{
		cnt:     30,
		success: true,
		body:    []byte{1, 2, 3},
		t:       t,
	}
	res, b, st = ov.retryOCSP(
		context.Background(),
		client, fakeRequestFunc,
		dummyOCSPHost,
		make(map[string]string), []byte{0}, certs[0], certs[len(certs)-1], 5*time.Second)
	if st.err == nil {
		fmt.Printf("should fail: %v, %v, %v\n", res, b, st)
	}
}

func TestFullOCSPURL(t *testing.T) {
	testcases := []tcFullOCSPURL{
		{
			url:               &url.URL{Host: "some-ocsp-url.com"},
			expectedURLString: "some-ocsp-url.com",
		},
		{
			url: &url.URL{
				Host: "some-ocsp-url.com",
				Path: "/some-path",
			},
			expectedURLString: "some-ocsp-url.com/some-path",
		},
		{
			url: &url.URL{
				Host: "some-ocsp-url.com",
				Path: "some-path",
			},
			expectedURLString: "some-ocsp-url.com/some-path",
		},
	}

	for _, testcase := range testcases {
		t.Run("", func(t *testing.T) {
			returnedStringURL := fullOCSPURL(testcase.url)
			if returnedStringURL != testcase.expectedURLString {
				t.Fatalf("failed to match returned OCSP url string; expected: %v, got: %v",
					testcase.expectedURLString, returnedStringURL)
			}
		})
	}
}

type tcFullOCSPURL struct {
	url               *url.URL
	expectedURLString string
}

func TestOCSPCacheServerRetry(t *testing.T) {
	dummyOCSPHost := &url.URL{
		Scheme: "https",
		Host:   "dummyOCSPHost",
	}
	client := &fakeHTTPClient{
		cnt:     3,
		success: true,
		body:    []byte{1, 2, 3},
		t:       t,
	}
	res, st := checkOCSPCacheServer(
		context.Background(), client, fakeRequestFunc, dummyOCSPHost, 20*time.Second)
	if st.err == nil {
		t.Errorf("should fail: %v", res)
	}
	client = &fakeHTTPClient{
		cnt:     30,
		success: true,
		body:    []byte{1, 2, 3},
		t:       t,
	}
	res, st = checkOCSPCacheServer(
		context.Background(), client, fakeRequestFunc, dummyOCSPHost, 10*time.Second)
	if st.err == nil {
		t.Errorf("should fail: %v", res)
	}
}

// TestExtractTsAndOcspRespBase64Malformed is a regression test for
// SNOW-3649896: a short or malformed cache entry (decoded from an external
// cache payload) must not panic with index-out-of-range; it is rejected
// instead.
func TestExtractTsAndOcspRespBase64Malformed(t *testing.T) {
	cases := []struct {
		name  string
		value []any
		ok    bool
	}{
		{"empty", []any{}, false},
		{"single element", []any{123.0}, false},
		{"ts wrong type", []any{"notFloat", "resp"}, false},
		{"resp wrong type", []any{123.0, 456.0}, false},
		{"well formed", []any{123.0, "respBase64"}, true},
		{"extra elements tolerated", []any{123.0, "respBase64", "ignored"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, _, _ := extractTsAndOcspRespBase64(tc.value) // must not panic
			assertEqualE(t, ok, tc.ok)
		})
	}
}

// TestCheckOCSPCacheServerMalformedPayloadNoPanic is a regression test for
// SNOW-3649896: a malformed OCSP cache body (short arrays) must be skipped
// without panicking inside the TLS verify callback.
func TestCheckOCSPCacheServerMalformedPayloadNoPanic(t *testing.T) {
	dummyOCSPHost := &url.URL{Scheme: "https", Host: "dummyOCSPHost"}
	// `[]` and `[123.0]` previously triggered index-out-of-range; the last
	// entry is well-formed and must survive.
	body := []byte(`{"emptyArr":[],"oneElem":[123.0],"goodEntry":[123.0,"abc"]}`)
	client := &fakeHTTPClient{cnt: 0, success: true, body: body, t: t}
	ret, st := checkOCSPCacheServer(
		context.Background(), client, fakeRequestFunc, dummyOCSPHost, 20*time.Second)
	assertEqualE(t, st.code, ocspSuccess)
	assertNotNilF(t, ret)
	assertEqualE(t, len(*ret), 1) // only the well-formed entry is kept
}

type tcCanEarlyExit struct {
	results       []*ocspStatus
	resultLen     int
	retFailOpen   *ocspStatus
	retFailClosed *ocspStatus
}

func TestCanEarlyExitForOCSP(t *testing.T) {
	testcases := []tcCanEarlyExit{
		{ // 0
			results: []*ocspStatus{
				{
					code: ocspStatusGood,
				},
				{
					code: ocspStatusGood,
				},
				{
					code: ocspStatusGood,
				},
			},
			retFailOpen:   nil,
			retFailClosed: nil,
		},
		{ // 1
			results: []*ocspStatus{
				{
					code: ocspStatusRevoked,
					err:  errors.New("revoked"),
				},
				{
					code: ocspStatusGood,
				},
				{
					code: ocspStatusGood,
				},
			},
			retFailOpen:   &ocspStatus{ocspStatusRevoked, errors.New("revoked")},
			retFailClosed: &ocspStatus{ocspStatusRevoked, errors.New("revoked")},
		},
		{ // 2
			results: []*ocspStatus{
				{
					code: ocspStatusUnknown,
					err:  errors.New("unknown"),
				},
				{
					code: ocspStatusGood,
				},
				{
					code: ocspStatusGood,
				},
			},
			retFailOpen:   nil,
			retFailClosed: &ocspStatus{ocspStatusUnknown, errors.New("unknown")},
		},
		{ // 3: a confirmed Revoked leaf must fail-closed even when a sibling response is invalid (ocspInvalidValidity).
			results: []*ocspStatus{
				{
					code: ocspStatusRevoked,
					err:  errors.New("revoked"),
				},
				{
					code: ocspInvalidValidity,
				},
				{
					code: ocspStatusGood,
				},
			},
			retFailOpen:   &ocspStatus{ocspStatusRevoked, errors.New("revoked")},
			retFailClosed: &ocspStatus{ocspStatusRevoked, errors.New("revoked")},
		},
		{ // 4: a confirmed Revoked leaf must fail-closed even when the number of results doesn't match the chain length.
			results: []*ocspStatus{
				{
					code: ocspStatusRevoked,
					err:  errors.New("revoked"),
				},
				{
					code: ocspStatusGood,
				},
			},
			resultLen:     3,
			retFailOpen:   &ocspStatus{ocspStatusRevoked, errors.New("revoked")},
			retFailClosed: &ocspStatus{ocspStatusRevoked, errors.New("revoked")},
		},
	}

	for idx, tt := range testcases {
		t.Run("", func(t *testing.T) {
			ovOpen := newOcspValidator(&Config{OCSPFailOpen: OCSPFailOpenTrue})
			expectedLen := len(tt.results)
			if tt.resultLen > 0 {
				expectedLen = tt.resultLen
			}
			expectedLen++ // add one because normally there is a root certificate that is not included in the results.
			mockVerifiedChain := make([]*x509.Certificate, expectedLen)
			r := ovOpen.canEarlyExitForOCSP(tt.results, mockVerifiedChain)
			if !(tt.retFailOpen == nil && r == nil) && !(tt.retFailOpen != nil && r != nil && tt.retFailOpen.code == r.code) {
				t.Fatalf("%d: failed to match return. expected: %v, got: %v", idx, tt.retFailOpen, r)
			}
			ovClosed := newOcspValidator(&Config{OCSPFailOpen: OCSPFailOpenFalse})
			r = ovClosed.canEarlyExitForOCSP(tt.results, mockVerifiedChain)
			if !(tt.retFailClosed == nil && r == nil) && !(tt.retFailClosed != nil && r != nil && tt.retFailClosed.code == r.code) {
				t.Fatalf("%d: failed to match return. expected: %v, got: %v", idx, tt.retFailClosed, r)
			}
		})
	}
}

// TestCanEarlyExitForOCSPRevokedWithSiblingError is a regression test for
// SNOW-3649733: in fail-open mode, a confirmed Revoked verdict for one chain
// element must still close the connection even when a different element could
// not be reached. Fail-open only covers inconclusive results, never a
// positively confirmed revocation.
func TestCanEarlyExitForOCSPRevokedWithSiblingError(t *testing.T) {
	siblingErrors := []ocspStatusCode{
		ocspFailedSubmit,
		ocspFailedResponse,
		ocspNoServer,
		ocspMissedCache,
	}
	for _, siblingCode := range siblingErrors {
		t.Run(fmt.Sprintf("revoked_leaf_sibling_%d", siblingCode), func(t *testing.T) {
			results := []*ocspStatus{
				{code: ocspStatusRevoked, err: errors.New("revoked")},
				{code: siblingCode, err: errors.New("responder unreachable")},
			}
			// chain = leaf + intermediate + root (root is not checked).
			mockVerifiedChain := make([]*x509.Certificate, 3)

			ovOpen := newOcspValidator(&Config{OCSPFailOpen: OCSPFailOpenTrue})
			r := ovOpen.canEarlyExitForOCSP(results, mockVerifiedChain)
			assertNotNilF(t, r, "fail-open must surface the Revoked verdict")
			assertEqualF(t, r.code, ocspStatusRevoked, "fail-open must surface the Revoked verdict")

			ovClosed := newOcspValidator(&Config{OCSPFailOpen: OCSPFailOpenFalse})
			r = ovClosed.canEarlyExitForOCSP(results, mockVerifiedChain)
			assertNotNilF(t, r, "fail-closed must reject the connection")
		})
	}
}

// TestCanEarlyExitForOCSPInvalidResponseFailsClosed covers SNOW-3649697: a
// response that was received but did not verify against the certificate being
// checked (ocspFailedParseResponse) is a definitive result and must close the
// connection in both fail-open and fail-closed modes, unlike an unreachable
// responder which fail-open tolerates.
func TestCanEarlyExitForOCSPInvalidResponseFailsClosed(t *testing.T) {
	results := []*ocspStatus{
		{code: ocspFailedParseResponse, err: errors.New("response did not validate")},
		{code: ocspStatusGood},
	}
	mockVerifiedChain := make([]*x509.Certificate, 3)

	ovOpen := newOcspValidator(&Config{OCSPFailOpen: OCSPFailOpenTrue})
	r := ovOpen.canEarlyExitForOCSP(results, mockVerifiedChain)
	assertNotNilF(t, r, "fail-open must not tolerate a response that did not validate")
	assertEqualF(t, r.code, ocspFailedParseResponse, "fail-open must not tolerate a response that did not validate")

	ovClosed := newOcspValidator(&Config{OCSPFailOpen: OCSPFailOpenFalse})
	r = ovClosed.canEarlyExitForOCSP(results, mockVerifiedChain)
	assertNotNilF(t, r, "fail-closed must reject the connection")
}

func TestInitOCSPCacheFileCreation(t *testing.T) {
	if runningOnGithubAction() {
		t.Skip("cannot write to github file system")
	}
	dirName, err := os.UserHomeDir()
	if err != nil {
		t.Error(err)
	}
	srcFileName := dirName + "/.cache/snowflake/ocsp_response_cache.json"
	tmpFileName := srcFileName + "_tmp"
	dst, err := os.Create(tmpFileName)
	if err != nil {
		t.Error(err)
	}
	defer dst.Close()

	var src *os.File
	if _, err = os.Stat(srcFileName); errors.Is(err, os.ErrNotExist) {
		// file does not exist
		if err = os.MkdirAll(dirName+"/.cache/snowflake/", os.ModePerm); err != nil {
			t.Error(err)
		}
		if _, err = os.Create(srcFileName); err != nil {
			t.Error(err)
		}
	} else if err != nil {
		t.Error(err)
	} else {
		// file exists
		src, err = os.Open(srcFileName)
		if err != nil {
			t.Error(err)
		}
		defer src.Close()
		// copy original contents to temporary file
		if _, err = io.Copy(dst, src); err != nil {
			t.Error(err)
		}
		if err = os.Remove(srcFileName); err != nil {
			t.Error(err)
		}
	}

	// cleanup
	defer func() {
		src, _ = os.Open(tmpFileName)
		defer src.Close()
		dst, _ = os.OpenFile(srcFileName, os.O_WRONLY, readWriteFileMode)
		defer dst.Close()
		// copy temporary file contents back to original file
		if _, err = io.Copy(dst, src); err != nil {
			t.Fatal(err)
		}
		if err = os.Remove(tmpFileName); err != nil {
			t.Error(err)
		}
	}()

	initOCSPCache()
	if _, err = os.Stat(srcFileName); errors.Is(err, os.ErrNotExist) {
		t.Error(err)
	} else if err != nil {
		t.Error(err)
	}
}

func syncUpdateOcspResponseCache(f func()) {
	ocspResponseCacheLock.Lock()
	defer ocspResponseCacheLock.Unlock()
	f()
}

// issue#1818 - corrupt/mangled OCSP entries must be skipped both in the on-filesystem, pre-existing
// OCSP cache file, and in the one which is freshly downloaded from OCSP_CACHE_SERVER, and
// must not cause a panic on nil pointer dereference.
// After skipping the corrupt entry, the driver continues to validate online with OCSP Responder.

// Corrupt entry in file downloaded from OCSP_CACHE_SERVER
func TestUnitDownloadOCSPCacheServerCorruptKey(t *testing.T) {
	ocspCacheServerEnabled = true

	wiremock.registerMappings(t, newWiremockMapping("ocsp/corrupt_cache_key.json"))

	ov := newOcspValidator(&Config{OCSPFailOpen: OCSPFailOpenTrue})
	ov.cacheServerURL = fmt.Sprintf("%v/ocsp_response_cache.json", wiremock.baseURL())

	syncUpdateOcspResponseCache(func() {
		ocspResponseCache = make(map[certIDKey]*certCacheValue)
	})

	// Must not panic.
	ov.downloadOCSPCacheServer()

	func() {
		ocspResponseCacheLock.RLock()
		defer ocspResponseCacheLock.RUnlock()
		assertEqualF(t, len(ocspResponseCache), 0, "corrupt key must not be stored in cache")
	}()
}

// Corrupt entry in the on-disk OCSP cache file
func TestUnitInitOCSPCacheCorruptKey(t *testing.T) {
	ocspCacheServerEnabled = true

	// Build a JSON payload with a key that is valid base64 but does not decode
	// to a valid ASN.1 certID structure.
	corruptKey := base64.StdEncoding.EncodeToString([]byte("this-is-not-asn1"))
	payload, err := json.Marshal(map[string]any{
		corruptKey: []any{
			float64(time.Now().UTC().Unix()),
			base64.StdEncoding.EncodeToString([]byte("junk-not-ocsp")),
		},
	})
	assertNilF(t, err, "marshalling test payload")

	f, err := os.CreateTemp(t.TempDir(), "ocsp_cache_corrupt_*.json")
	assertNilF(t, err, "creating temp cache file")
	_, err = f.Write(payload)
	assertNilF(t, err, "writing temp cache file")
	assertNilF(t, f.Close(), "closing temp cache file")

	origCacheFileName := cacheFileName
	cacheFileName = f.Name()
	defer func() { cacheFileName = origCacheFileName }()

	// Must not panic.
	initOCSPCache()

	func() {
		ocspResponseCacheLock.RLock()
		defer ocspResponseCacheLock.RUnlock()
		assertEqualF(t, len(ocspResponseCache), 0, "corrupt key from file must not be stored in cache")
	}()
}
