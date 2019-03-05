// Copyright (c) 2017-2019 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"golang.org/x/crypto/ocsp"
)

func TestOCSP(t *testing.T) {
	os.Remove(cacheFileName) // clear cache file
	targetURL := []string{
		"https://sfctest0.snowflakecomputing.com/",
		"https://s3-us-west-2.amazonaws.com/sfc-snowsql-updates/?prefix=1.1/windows_x86_64",
	}

	transports := []*http.Transport{
		snowflakeInsecureTransport,
		SnowflakeTransportSerial,
		SnowflakeTransport,
	}

	for _, tgt := range targetURL {
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
			if res.StatusCode != http.StatusOK {
				t.Fatalf("failed to get 200: %v", res.StatusCode)
			}
			_, err = ioutil.ReadAll(res.Body)
			if err != nil {
				t.Fatalf("failed to read content body for %v", tgt)
			}

		}
	}
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
		"testaccount.snowflakecomputing.com:443",
		"s3-us-west-2.amazonaws.com:443",
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
			_, ost = encodeCertID(ocspReq)
			if ost.err != nil {
				t.Fatalf("failed to extract cert ID from the OCSP request. err: %v", ost.err)
			}
			// better hash. Not sure if the actual OCSP server accepts this, though.
			ocspReq, err = ocsp.CreateRequest(subject, issuer, &ocsp.RequestOptions{Hash: crypto.SHA512})
			if err != nil {
				t.Fatalf("failed to create OCSP request. err: %v", err)
			}
			_, ost = encodeCertID(ocspReq)
			if ost.err != nil {
				t.Fatalf("failed to extract cert ID from the OCSP request. err: %v", ost.err)
			}
			// tweaked request binary
			ocspReq, err = ocsp.CreateRequest(subject, issuer, &ocsp.RequestOptions{Hash: crypto.SHA512})
			if err != nil {
				t.Fatalf("failed to create OCSP request. err: %v", err)
			}
			ocspReq[10] = 0 // random change
			_, ost = encodeCertID(ocspReq)
			if ost.err == nil {
				t.Fatal("should have failed")
			}
		}
	}
}

func TestUnitCheckOCSPResponseCache(t *testing.T) {
	ocspResponseCache["DUMMY_KEY"] = []interface{}{float64(1395054952), "DUMMY_VALUE"}
	subject := &x509.Certificate{}
	issuer := &x509.Certificate{}
	ost := checkOCSPResponseCache([]byte("DUMMY_KEY"), subject, issuer)
	if ost.code != ocspMissedCache {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspMissedCache, ost.code)
	}
	// old timestamp
	ocspResponseCache["RFVNTVlfS0VZ"] = []interface{}{float64(1395054952), "DUMMY_VALUE"}
	ost = checkOCSPResponseCache([]byte("DUMMY_KEY"), subject, issuer)
	if ost.code != ocspCacheExpired {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspCacheExpired, ost.code)
	}
	// future timestamp
	ocspResponseCache["RFVNTVlfS0VZ"] = []interface{}{float64(1595054952), "DUMMY_VALUE"}
	ost = checkOCSPResponseCache([]byte("DUMMY_KEY"), subject, issuer)
	if ost.code != ocspFailedDecodeResponse {
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
	ocspResponseCache["RFVNTVlfS0VZ"] = []interface{}{float64(1595054952), actualOcspResponse}
	ost = checkOCSPResponseCache([]byte("DUMMY_KEY"), subject, issuer)
	if ost.code != ocspFailedParseResponse {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspFailedParseResponse, ost.code)
	}
	// wrong timestamp type
	ocspResponseCache["RFVNTVlfS0VZ"] = []interface{}{uint32(1595054952), 123456}
	ost = checkOCSPResponseCache([]byte("DUMMY_KEY"), subject, issuer)
	if ost.code != ocspMissedCache {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspMissedCache, ost.code)
	}
	// wrong value type
	ocspResponseCache["RFVNTVlfS0VZ"] = []interface{}{float64(1595054952), 123456}
	ost = checkOCSPResponseCache([]byte("DUMMY_KEY"), subject, issuer)
	if ost.code != ocspMissedCache {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspMissedCache, ost.code)
	}
}

func TestUnitValidateOCSP(t *testing.T) {
	subject := &x509.Certificate{}
	ocspRes := &ocsp.Response{}
	ost := validateOCSP("dummykey", ocspRes, subject)
	if ost.code != ocspInvalidValidity {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspInvalidValidity, ost.code)
	}
	currentTime := time.Now()
	ocspRes.ThisUpdate = currentTime.Add(-2 * time.Hour)
	ocspRes.NextUpdate = currentTime.Add(2 * time.Hour)
	ocspRes.Status = ocsp.Revoked
	ost = validateOCSP("dummykey", ocspRes, subject)
	if ost.code != ocspRevokedOrUnknown {
		t.Fatalf("should have failed. expected: %v, got: %v", ocspRevokedOrUnknown, ost.code)
	}
	ocspRes.Status = ocsp.Good
	ost = validateOCSP("dummykey", ocspRes, subject)
	if ost.code != ocspSuccess {
		t.Fatalf("should have success. expected: %v, got: %v", ocspSuccess, ost.code)
	}
}

func TestUnitEncodeCertID(t *testing.T) {
	var st *ocspStatus
	_, st = encodeCertID([]byte{0x1, 0x2})
	if st.code != ocspFailedDecomposeRequst {
		t.Fatalf("failed to get OCSP status. expected: %v, got: %v", ocspFailedDecomposeRequst, st.code)
	}
}

func getCert(addr string) []*x509.Certificate {
	tcpConn, err := net.DialTimeout("tcp", addr, 4*time.Second)
	if err != nil {
		panic(err)
	}
	defer tcpConn.Close()

	tcpConn.SetDeadline(time.Now().Add(1 * time.Second))
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
	client := &fakeHTTPClient{
		cnt:     3,
		success: true,
		body:    []byte{1, 2, 3},
	}
	res, b, st := retryOCSP(
		client, fakeRequestFunc,
		"dummyOCSPHost",
		make(map[string]string), []byte{0}, certs[len(certs)-1], 20*time.Second, 10*time.Second)
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
		"dummyOCSPHost",
		make(map[string]string), []byte{0}, certs[len(certs)-1], 10*time.Second, 5*time.Second)
	if st.err == nil {
		fmt.Printf("should fail: %v, %v, %v\n", res, b, st)
	}
}
