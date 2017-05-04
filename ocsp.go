// Package gosnowflake is a utility package for Go Snowflake Driver
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/golang/glog"
	"golang.org/x/crypto/ocsp"
)

const (
	tolerableValidityRatio = 100               // buffer for certificate revocation update time
	maxClockSkew           = 900 * time.Second // buffer for clock skew
)

type ocspStatus int

const (
	ocspSuccess               ocspStatus = 0
	ocspNoServer              ocspStatus = -1
	ocspFailedParseOCSPHost   ocspStatus = -2
	ocspFailedComposeRequest  ocspStatus = -3
	ocspFailedSubmit          ocspStatus = -4
	ocspFailedResponse        ocspStatus = -5
	ocspFailedExtractResponse ocspStatus = -6
	ocspFailedParseResponse   ocspStatus = -7
	ocspInvalidValidity       ocspStatus = -8
	ocspRevokedOrUnknown      ocspStatus = -9
)

func maxDuration(d1, d2 time.Duration) time.Duration {
	if d1-d2 > 0 {
		return d1
	}
	return d2
}

// calcTolerableValidity returns the maximum validity buffer
func calcTolerableValidity(thisUpdate, nextUpdate time.Time) time.Duration {
	return maxDuration(time.Duration(nextUpdate.Sub(thisUpdate)/tolerableValidityRatio), maxClockSkew)
}

// isInValidityRange checks the validity
func isInValidityRange(currTime, thisUpdate, nextUpdate time.Time) bool {
	if thisUpdate.Add(-maxClockSkew).Sub(currTime) > 0 {
		return false
	}
	if nextUpdate.Add(calcTolerableValidity(thisUpdate, nextUpdate)).Sub(currTime) < 0 {
		return false
	}
	return true
}

// getRevocationStatus checks the certificate revocation status for subject using issuer certificate.
func getRevocationStatus(wg *sync.WaitGroup, ocspStatusChan chan<- ocspStatus, ocspErrorChan chan<- error, subject, issuer *x509.Certificate) {
	defer wg.Done()
	glog.V(2).Infof("Subject: %v\n", subject.Subject)
	glog.V(2).Infof("Issuer:  %v\n", issuer.Subject)
	glog.V(2).Infof("OCSP Server: %v\n", subject.OCSPServer)
	if len(subject.OCSPServer) == 0 {
		ocspErrorChan <- fmt.Errorf("no OCSP server is attached to the certificate. %v", subject.Subject)
		ocspStatusChan <- ocspNoServer
		return
	}
	ocspHost := subject.OCSPServer[0]
	u, err := url.Parse(ocspHost)
	if err != nil {
		ocspErrorChan <- fmt.Errorf("failed to parse OCSP server host. %v", ocspHost)
		ocspStatusChan <- ocspFailedParseOCSPHost
		return
	}
	ocspClient := &http.Client{
		Timeout: 30 * time.Second,
	}
	ocspReq, err := ocsp.CreateRequest(subject, issuer, &ocsp.RequestOptions{})
	if err != nil {
		ocspErrorChan <- fmt.Errorf("failed to compose OCSP request object. %v", subject.Subject)
		ocspStatusChan <- ocspFailedComposeRequest
		return
	}

	headers := make(map[string]string)
	headers["Content-Type"] = "application/ocsp-request"
	headers["Content-Length"] = string(len(ocspReq))
	headers["Host"] = u.Hostname()
	res, err := retryHTTP(context.Background(), ocspClient, http.NewRequest, "POST", ocspHost, headers, ocspReq, 30*time.Second)
	if err != nil {
		ocspErrorChan <- err
		ocspStatusChan <- ocspFailedSubmit
		return
	}
	defer res.Body.Close()
	glog.V(2).Infof("StatusCode from OCSP Server: %v", res.StatusCode)
	if res.StatusCode != http.StatusOK {
		ocspErrorChan <- fmt.Errorf("HTTP code is not OK. %v: %v", res.StatusCode, res.Status)
		ocspStatusChan <- ocspFailedResponse
		return
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		ocspErrorChan <- err
		ocspStatusChan <- ocspFailedExtractResponse
		return
	}
	ocspRes, err := ocsp.ParseResponse(b, issuer)
	if err != nil {
		ocspErrorChan <- err
		ocspStatusChan <- ocspFailedParseResponse
		return
	}
	curTime := time.Now()
	if !isInValidityRange(curTime, ocspRes.ThisUpdate, ocspRes.NextUpdate) {
		ocspErrorChan <- fmt.Errorf("invalid validity: producedAt: %v, thisUpdate: %v, nextUpdate: %v", ocspRes.ProducedAt, ocspRes.ThisUpdate, ocspRes.NextUpdate)
		ocspStatusChan <- ocspInvalidValidity
		return
	}
	if ocspRes.Status != ocsp.Good {
		ocspErrorChan <- fmt.Errorf("bad revocation status. %v: %v, cert: %v", ocspRes.Status, ocspRes.RevocationReason, subject.Subject)
		ocspStatusChan <- ocspRevokedOrUnknown
		return
	}
	ocspStatusChan <- ocspSuccess
	return
}

// verifyPeerCertificateSerial verifies the certificate revocation status in serial.
// This is mainly used by tools that analyzes the OCSP output
func verifyPeerCertificateSerial(_ [][]byte, verifiedChains [][]*x509.Certificate) (err error) {
	for i := 0; i < len(verifiedChains); i++ {
		var wg sync.WaitGroup
		n := len(verifiedChains[i]) - 1
		wg.Add(n)
		for j := 0; j < len(verifiedChains[i])-1; j++ {
			ocspStatusChan := make(chan ocspStatus, 1)
			ocspErrorChan := make(chan error, 1)
			getRevocationStatus(&wg, ocspStatusChan, ocspErrorChan, verifiedChains[i][j], verifiedChains[i][j+1])
			close(ocspErrorChan)
			close(ocspStatusChan)
			st := <-ocspStatusChan
			if st != 0 {
				e := <-ocspErrorChan
				return fmt.Errorf("failed to validate the certificate revocation status. err: %v", e)
			}
		}
		wg.Wait()
	}
	return nil
}

// verifyPeerCertificateParallel verifies the certificate revocation status in parallel.
// This is mainly used for general connection
func verifyPeerCertificateParallel(_ [][]byte, verifiedChains [][]*x509.Certificate) (err error) {
	for i := 0; i < len(verifiedChains); i++ {
		var wg sync.WaitGroup
		n := len(verifiedChains[i]) - 1
		wg.Add(n)
		ocspStatusChan := make(chan ocspStatus, n)
		ocspErrorChan := make(chan error, n)
		for j := 0; j < n; j++ {
			go getRevocationStatus(&wg, ocspStatusChan, ocspErrorChan, verifiedChains[i][j], verifiedChains[i][j+1])
		}
		results := make([]ocspStatus, n)
		for j := 0; j < n; j++ {
			results[j] = <-ocspStatusChan // will wait for all results back
		}
		close(ocspErrorChan)
		close(ocspStatusChan)
		wg.Wait()
		for _, r := range results {
			if r != ocspSuccess {
				e := <-ocspErrorChan
				return fmt.Errorf("failed certificate revocation check. err: %v", e)
			}
		}
	}
	return nil
}

var snowflakeTransport = &http.Transport{
	TLSClientConfig: &tls.Config{
		VerifyPeerCertificate: verifyPeerCertificateParallel,
	},
	MaxIdleConns:    10,
	IdleConnTimeout: 30 * time.Minute,
}

// SnowflakeTransportSerial includes the certificate revocation check
var SnowflakeTransportSerial = &http.Transport{
	TLSClientConfig: &tls.Config{
		VerifyPeerCertificate: verifyPeerCertificateSerial,
	},
	MaxIdleConns:    10,
	IdleConnTimeout: 30 * time.Minute,
}

// SnowflakeTransportTest includes the certificate revocation check in parallel
var SnowflakeTransportTest = &http.Transport{
	TLSClientConfig: &tls.Config{
		VerifyPeerCertificate: verifyPeerCertificateParallel,
	},
	MaxIdleConns:    10,
	IdleConnTimeout: 30 * time.Minute,
}
