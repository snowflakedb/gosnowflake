// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"golang.org/x/crypto/ocsp"
)

func TestOCSP(t *testing.T) {
	cacheServerEnabled := []string{
		"true",
		"false",
	}
	targetURL := []string{
		"https://sfctest0.snowflakecomputing.com/",
		"https://s3-us-west-2.amazonaws.com/sfc-snowsql-updates/?prefix=1.1/windows_x86_64",
		"https://sfcdev1.blob.core.windows.net/",
	}

	transports := []*http.Transport{
		snowflakeInsecureTransport,
		SnowflakeTransport,
	}

	for _, enabled := range cacheServerEnabled {
		for _, tgt := range targetURL {
			_ = os.Setenv(cacheServerEnabledEnv, enabled)
			_ = os.Remove(cacheFileName) // clear cache file
			ocspResponseCache = make(map[certIDKey][]interface{})
			for _, tr := range transports {
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
				_, err = ioutil.ReadAll(res.Body)
				if err != nil {
					t.Fatalf("failed to read content body for %v", tgt)
				}

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
		if tc.ret != isInValidityRange(currentTime, tc.thisTime, tc.nextTime) {
			t.Fatalf("failed to check validity. should be: %v, currentTime: %v, thisTime: %v, nextTime: %v", tc.ret, currentTime, tc.thisTime, tc.nextTime)
		}
	}
}

func TestUnitEncodeCertIDGood(t *testing.T) {
	targetURLs := []string{
		"faketestaccount.snowflakecomputing.com:443",
		"s3-us-west-2.amazonaws.com:443",
		"sfcdev1.blob.core.windows.net:443",
	}
	for _, tt := range targetURLs {
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
	}
}

func TestUnitCheckOCSPResponseCache(t *testing.T) {
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
	ocspResponseCache[dummyKey0] = []interface{}{currentTime, b64Key}
	subject := &x509.Certificate{}
	issuer := &x509.Certificate{}
	ost := checkOCSPResponseCache(&dummyKey, subject, issuer)
	if ost.code != ocspMissedCache {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspMissedCache, ost.code)
	}
	// old timestamp
	ocspResponseCache[dummyKey] = []interface{}{float64(1395054952), b64Key}
	ost = checkOCSPResponseCache(&dummyKey, subject, issuer)
	if ost.code != ocspCacheExpired {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspCacheExpired, ost.code)
	}
	// future timestamp
	ocspResponseCache[dummyKey] = []interface{}{float64(1805054952), b64Key}
	ost = checkOCSPResponseCache(&dummyKey, subject, issuer)
	if ost.code != ocspFailedParseResponse {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspFailedDecodeResponse, ost.code)
	}
	// actual OCSP but it fails to parse, because an invalid issuer certificate is given.
	actualOcspResponse := "MIIB0woBAKCCAcwwggHIBgkrBgEFBQcwAQEEggG5MIIBtTCBnqIWBBSxPsNpA/i/RwHUmCYaCALvY2QrwxgPMjAxNz" +
		"A1MTYyMjAwMDBaMHMwcTBJMAkGBSsOAwIaBQAEFN+qEuMosQlBk+KfQoLOR0BClVijBBSxPsNpA/i/RwHUmCYaCALvY2QrwwIQBOHnp" +
		"Nxc8vNtwCtCuF0Vn4AAGA8yMDE3MDUxNjIyMDAwMFqgERgPMjAxNzA1MjMyMjAwMDBaMA0GCSqGSIb3DQEBCwUAA4IBAQCuRGwqQsKy" +
		"IAAGHgezTfG0PzMYgGD/XRDhU+2i08WTJ4Zs40Lu88cBeRXWF3iiJSpiX3/OLgfI7iXmHX9/sm2SmeNWc0Kb39bk5Lw1jwezf8hcI9+" +
		"mZHt60vhUgtgZk21SsRlTZ+S4VXwtDqB1Nhv6cnSnfrL2A9qJDZS2ltPNOwebWJnznDAs2dg+KxmT2yBXpHM1kb0EOolWvNgORbgIgB" +
		"koRzw/UU7zKsqiTB0ZN/rgJp+MocTdqQSGKvbZyR8d4u8eNQqi1x4Pk3yO/pftANFaJKGB+JPgKS3PQAqJaXcipNcEfqtl7y4PO6kqA" +
		"Jb4xI/OTXIrRA5TsT4cCioE"
	// issuer is not a true issuer certificate
	ocspResponseCache[dummyKey] = []interface{}{float64(1595054952), actualOcspResponse}
	ost = checkOCSPResponseCache(&dummyKey, subject, issuer)
	if ost.code != ocspFailedParseResponse {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspFailedParseResponse, ost.code)
	}
	// invalid validity
	ocspResponseCache[dummyKey] = []interface{}{float64(1595054952), actualOcspResponse}
	ost = checkOCSPResponseCache(&dummyKey, subject, nil)
	if ost.code != ocspInvalidValidity {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspInvalidValidity, ost.code)
	}
	// wrong timestamp type
	ocspResponseCache[dummyKey] = []interface{}{uint32(1595054952), 123456}
	ost = checkOCSPResponseCache(&dummyKey, subject, issuer)
	if ost.code != ocspFailedDecodeResponse {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspFailedDecodeResponse, ost.code)
	}
	// wrong value type
	ocspResponseCache[dummyKey] = []interface{}{float64(1595054952), 123456}
	ost = checkOCSPResponseCache(&dummyKey, subject, issuer)
	if ost.code != ocspFailedDecodeResponse {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspFailedDecodeResponse, ost.code)
	}
}

func TestUnitValidateOCSP(t *testing.T) {
	ocspRes := &ocsp.Response{}
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
	certs := getCert("s3-us-west-2.amazonaws.com:443")
	dummyOCSPHost := &url.URL{
		Scheme: "https",
		Host:   "dummyOCSPHost",
	}
	client := &fakeHTTPClient{
		cnt:     3,
		success: true,
		body:    []byte{1, 2, 3},
	}
	res, b, st := retryOCSP(
		client, fakeRequestFunc,
		dummyOCSPHost,
		make(map[string]string), []byte{0}, certs[len(certs)-1], 10*time.Second)
	if st.err == nil {
		fmt.Printf("should fail: %v, %v, %v\n", res, b, st)
	}
	client = &fakeHTTPClient{
		cnt:     30,
		success: true,
		body:    []byte{1, 2, 3},
	}
	res, b, st = retryOCSP(
		client, fakeRequestFunc,
		dummyOCSPHost,
		make(map[string]string), []byte{0}, certs[len(certs)-1], 5*time.Second)
	if st.err == nil {
		fmt.Printf("should fail: %v, %v, %v\n", res, b, st)
	}
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
	}
	res, st := checkOCSPCacheServer(
		client, fakeRequestFunc, dummyOCSPHost, 20*time.Second)
	if st.err == nil {
		t.Errorf("should fail: %v", res)
	}
	client = &fakeHTTPClient{
		cnt:     30,
		success: true,
		body:    []byte{1, 2, 3},
	}
	res, st = checkOCSPCacheServer(
		client, fakeRequestFunc, dummyOCSPHost, 10*time.Second)
	if st.err == nil {
		t.Errorf("should fail: %v", res)
	}
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
		{ // 3: not taken as revoked if any invalid OCSP response (ocspInvalidValidity) is included.
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
			retFailOpen:   nil,
			retFailClosed: &ocspStatus{ocspStatusRevoked, errors.New("revoked")},
		},
		{ // 4: not taken as revoked if the number of results don't match the expected results.
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
			retFailOpen:   nil,
			retFailClosed: &ocspStatus{ocspStatusRevoked, errors.New("revoked")},
		},
	}

	for idx, tt := range testcases {
		ocspFailOpen = OCSPFailOpenTrue
		expectedLen := len(tt.results)
		if tt.resultLen > 0 {
			expectedLen = tt.resultLen
		}
		r := canEarlyExitForOCSP(tt.results, expectedLen)
		if !(tt.retFailOpen == nil && r == nil) && !(tt.retFailOpen != nil && r != nil && tt.retFailOpen.code == r.code) {
			t.Fatalf("%d: failed to match return. expected: %v, got: %v", idx, tt.retFailOpen, r)
		}
		ocspFailOpen = OCSPFailOpenFalse
		r = canEarlyExitForOCSP(tt.results, expectedLen)
		if !(tt.retFailClosed == nil && r == nil) && !(tt.retFailClosed != nil && r != nil && tt.retFailClosed.code == r.code) {
			t.Fatalf("%d: failed to match return. expected: %v, got: %v", idx, tt.retFailClosed, r)
		}
	}
}
