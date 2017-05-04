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

type ocspStatusCode int

type ocspStatus struct {
	code ocspStatusCode
	err  error
}

const (
	ocspSuccess               ocspStatusCode = 0
	ocspNoServer              ocspStatusCode = -1
	ocspFailedParseOCSPHost   ocspStatusCode = -2
	ocspFailedComposeRequest  ocspStatusCode = -3
	ocspFailedSubmit          ocspStatusCode = -4
	ocspFailedResponse        ocspStatusCode = -5
	ocspFailedExtractResponse ocspStatusCode = -6
	ocspFailedParseResponse   ocspStatusCode = -7
	ocspInvalidValidity       ocspStatusCode = -8
	ocspRevokedOrUnknown      ocspStatusCode = -9
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

func retryRevocationStatusCheck(retryCounter *int, totalTimeout *int, sleepTime int) (ok bool) {
	if *totalTimeout > 0 {
		*totalTimeout -= sleepTime
	}
	if *totalTimeout <= 0 {
		return false
	}
	time.Sleep(time.Duration(sleepTime) * time.Second)
	*retryCounter++
	return true
}

// getRevocationStatus checks the certificate revocation status for subject using issuer certificate.
func getRevocationStatus(wg *sync.WaitGroup, ocspStatusChan chan<- *ocspStatus, subject, issuer *x509.Certificate) {
	defer wg.Done()
	glog.V(2).Infof("Subject: %v\n", subject.Subject)
	glog.V(2).Infof("Issuer:  %v\n", issuer.Subject)
	glog.V(2).Infof("OCSP Server: %v\n", subject.OCSPServer)
	if len(subject.OCSPServer) == 0 {
		ocspStatusChan <- &ocspStatus{
			code: ocspNoServer,
			err:  fmt.Errorf("no OCSP server is attached to the certificate. %v", subject.Subject),
		}
		return
	}
	ocspHost := subject.OCSPServer[0]
	u, err := url.Parse(ocspHost)
	if err != nil {
		ocspStatusChan <- &ocspStatus{
			code: ocspFailedParseOCSPHost,
			err:  fmt.Errorf("failed to parse OCSP server host. %v", ocspHost),
		}
		return
	}
	ocspClient := &http.Client{
		Timeout: 30 * time.Second,
	}
	ocspReq, err := ocsp.CreateRequest(subject, issuer, &ocsp.RequestOptions{})
	if err != nil {
		ocspStatusChan <- &ocspStatus{
			code: ocspFailedComposeRequest,
			err:  fmt.Errorf("failed to compose OCSP request object. %v", subject.Subject),
		}
		return
	}

	headers := make(map[string]string)
	headers["Content-Type"] = "application/ocsp-request"
	headers["Accept"] = "application/ocsp-response"
	headers["Content-Length"] = string(len(ocspReq))
	headers["Host"] = u.Hostname()
	retryCounter := 0
	sleepTime := 0
	totalTimeout := 120
	var ocspRes *ocsp.Response
	for {
		sleepTime = defaultWaitAlgo.decorr(retryCounter, sleepTime)
		res, err := retryHTTP(context.Background(), ocspClient, http.NewRequest, "POST", ocspHost, headers, ocspReq, 30*time.Second)
		if err != nil {
			if ok := retryRevocationStatusCheck(&retryCounter, &totalTimeout, sleepTime); ok {
				continue
			}
			ocspStatusChan <- &ocspStatus{
				code: ocspFailedSubmit,
				err:  err,
			}
			return
		}
		defer res.Body.Close()
		glog.V(2).Infof("StatusCode from OCSP Server: %v", res.StatusCode)
		if res.StatusCode != http.StatusOK {
			if ok := retryRevocationStatusCheck(&retryCounter, &totalTimeout, sleepTime); ok {
				retryCounter++
				continue
			}
			ocspStatusChan <- &ocspStatus{
				code: ocspFailedResponse,
				err:  fmt.Errorf("HTTP code is not OK. %v: %v", res.StatusCode, res.Status),
			}
			return
		}
		b, err := ioutil.ReadAll(res.Body)
		if err != nil {
			if ok := retryRevocationStatusCheck(&retryCounter, &totalTimeout, sleepTime); ok {
				retryCounter++
				continue
			}
			ocspStatusChan <- &ocspStatus{
				code: ocspFailedExtractResponse,
				err:  err,
			}
			return
		}
		ocspRes, err = ocsp.ParseResponse(b, issuer)
		if err != nil {
			if ok := retryRevocationStatusCheck(&retryCounter, &totalTimeout, sleepTime); ok {
				retryCounter++
				continue
			}
			ocspStatusChan <- &ocspStatus{
				code: ocspFailedParseResponse,
				err:  err,
			}
			return
		}
		break
	}
	curTime := time.Now()
	if !isInValidityRange(curTime, ocspRes.ThisUpdate, ocspRes.NextUpdate) {
		ocspStatusChan <- &ocspStatus{
			code: ocspInvalidValidity,
			err:  fmt.Errorf("invalid validity: producedAt: %v, thisUpdate: %v, nextUpdate: %v", ocspRes.ProducedAt, ocspRes.ThisUpdate, ocspRes.NextUpdate),
		}
		return
	}
	if ocspRes.Status != ocsp.Good {
		ocspStatusChan <- &ocspStatus{
			code: ocspRevokedOrUnknown,
			err:  fmt.Errorf("bad revocation status. %v: %v, cert: %v", ocspRes.Status, ocspRes.RevocationReason, subject.Subject),
		}
		return
	}
	ocspStatusChan <- &ocspStatus{
		code: ocspSuccess,
		err:  nil,
	}
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
			ocspStatusChan := make(chan *ocspStatus, 1)
			getRevocationStatus(&wg, ocspStatusChan, verifiedChains[i][j], verifiedChains[i][j+1])
			close(ocspStatusChan)
			st := <-ocspStatusChan
			if st.code != 0 {
				return fmt.Errorf("failed to validate the certificate revocation status. err: %v", st.err)
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
		ocspStatusChan := make(chan *ocspStatus, n)
		for j := 0; j < n; j++ {
			go getRevocationStatus(&wg, ocspStatusChan, verifiedChains[i][j], verifiedChains[i][j+1])
		}
		results := make([]*ocspStatus, n)
		for j := 0; j < n; j++ {
			results[j] = <-ocspStatusChan // will wait for all results back
		}
		close(ocspStatusChan)
		wg.Wait()
		for _, r := range results {
			if r.code != ocspSuccess {
				return fmt.Errorf("failed certificate revocation check. err: %v", r.err)
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
