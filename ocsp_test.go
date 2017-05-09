// Package gosnowflake is a utility package for Go Snowflake Driver
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
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
		snowflakeTransport,
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

func TestIsInValidityRange(t *testing.T) {
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

func TestOCSPCertID(t *testing.T) {
	targetURLs := []string{
		"testaccount.snowflakecomputing.com:443",
		"s3-us-west-2.amazonaws.com:443",
	}
	for _, tt := range targetURLs {
		chainedCerts := getCert(tt)
		for _, c := range chainedCerts {
			ocspServers := c.OCSPServer
			if len(ocspServers) == 0 {
				break
			}
			//ocsp.CreateRequest(c, chainedCerts[idx+1], &ocsp.RequestOptions{})
		}
	}
}

func getCert(addr string) []*x509.Certificate {
	tcpConn, err := net.DialTimeout("tcp", addr, 4*time.Second)
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
	client := &fakeClient{
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
	client = &fakeClient{
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
