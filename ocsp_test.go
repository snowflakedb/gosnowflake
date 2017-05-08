// Package gosnowflake is a utility package for Go Snowflake Driver
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestOCSP(t *testing.T) {
	os.Remove(cacheFileName)
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

/*
func TestOCSPCertID(t *testing.T) {

	targetURLs := []string{
		"testaccount.snowflakecomputing.com:443",
		"s3-us-west-2.amazonaws.com:443",
	}
	for _, tt := range targetURLs {
		chainedCerts := getCert(tt)
		for _, c := range chainedCerts {
			fmt.Printf("%v, %v\n", c.Subject, c.OCSPServer)
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
*/
