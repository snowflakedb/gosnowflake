// Package gosnowflake is a utility package for Go Snowflake Driver
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/golang/glog"
	"golang.org/x/crypto/ocsp"
)

// CacheDir is the location of OCSP Response cache file ocsp_response_cache.
var CacheDir = ""

const (
	// CacheFileName is the name of OCSP Response cache file.
	CacheFileName = "ocsp_response_cache"
	// cacheExpire specifies cache data expiration time in seconds.
	cacheExpire = float64(24 * 60 * 60)
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
	ocspFailedDecomposeRequst ocspStatusCode = -4
	ocspFailedEncodeCertID    ocspStatusCode = -5
	ocspFailedSubmit          ocspStatusCode = -6
	ocspFailedResponse        ocspStatusCode = -7
	ocspFailedExtractResponse ocspStatusCode = -8
	ocspFailedParseResponse   ocspStatusCode = -9
	ocspInvalidValidity       ocspStatusCode = -10
	ocspRevokedOrUnknown      ocspStatusCode = -11
	ocspMissedCache           ocspStatusCode = -12
	ocspCacheExpired          ocspStatusCode = -13
	ocspFailedDecodeResponse  ocspStatusCode = -14
)

var (
	ocspResponseCache     map[string][]interface{}
	ocspResponseCacheLock *sync.RWMutex
)

// copied from crypto/ocsp
var hashOIDs = map[crypto.Hash]asn1.ObjectIdentifier{
	crypto.SHA1:   asn1.ObjectIdentifier([]int{1, 3, 14, 3, 2, 26}),
	crypto.SHA256: asn1.ObjectIdentifier([]int{2, 16, 840, 1, 101, 3, 4, 2, 1}),
	crypto.SHA384: asn1.ObjectIdentifier([]int{2, 16, 840, 1, 101, 3, 4, 2, 2}),
	crypto.SHA512: asn1.ObjectIdentifier([]int{2, 16, 840, 1, 101, 3, 4, 2, 3}),
}

// copied from crypto/ocsp.go
type certID struct {
	HashAlgorithm pkix.AlgorithmIdentifier
	NameHash      []byte
	IssuerKeyHash []byte
	SerialNumber  *big.Int
}

// copied from crypto/ocsp.go
func getOIDFromHashAlgorithm(target crypto.Hash) asn1.ObjectIdentifier {
	for hash, oid := range hashOIDs {
		if hash == target {
			return oid
		}
	}
	return nil
}

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

func encodeCertID(ocspReq []byte) ([]byte, *ocspStatus) {
	r, err := ocsp.ParseRequest(ocspReq)
	if err != nil {
		return nil, &ocspStatus{
			code: ocspFailedDecomposeRequst,
			err:  err,
		}
	}

	// encode CertID, used as a key in the cache
	hashAlg := getOIDFromHashAlgorithm(r.HashAlgorithm)
	encodedCertID, err := asn1.Marshal(certID{
		pkix.AlgorithmIdentifier{
			Algorithm:  hashAlg,
			Parameters: asn1.RawValue{Tag: 5 /* ASN.1 NULL */},
		},
		r.IssuerNameHash,
		r.IssuerKeyHash,
		r.SerialNumber,
	})
	if err != nil {
		return nil, &ocspStatus{
			code: ocspFailedEncodeCertID,
			err:  err,
		}
	}
	return encodedCertID, &ocspStatus{
		code: ocspSuccess,
	}
}

func checkOCSPResponseCache(encodedCertID []byte, subject, issuer *x509.Certificate) *ocspStatus {
	encodedCertIDBase64 := base64.StdEncoding.EncodeToString(encodedCertID)
	ocspResponseCacheLock.Lock()
	gotValueFromCache := ocspResponseCache[encodedCertIDBase64]
	ocspResponseCacheLock.Unlock()
	if len(gotValueFromCache) != 2 {
		return &ocspStatus{
			code: ocspMissedCache,
			err:  fmt.Errorf("miss cache data. CertID: %v", encodedCertIDBase64),
		}
	}
	// hit cache!
	currentTime := float64(time.Now().UTC().Unix())
	if epoch, ok := gotValueFromCache[0].(float64); ok {
		if currentTime-epoch >= cacheExpire {
			ocspResponseCacheLock.Lock()
			delete(ocspResponseCache, encodedCertIDBase64)
			ocspResponseCacheLock.Unlock()
			return &ocspStatus{
				code: ocspCacheExpired,
				err: fmt.Errorf("cache expired. current: %v, cache: %v, CertID: %v",
					time.Unix(int64(currentTime), 0).UTC(), time.Unix(int64(epoch), 0).UTC(), encodedCertIDBase64),
			}
		}
		if s, ok := gotValueFromCache[1].(string); ok {
			b, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				ocspResponseCacheLock.Lock()
				delete(ocspResponseCache, encodedCertIDBase64)
				ocspResponseCacheLock.Unlock()
				return &ocspStatus{
					code: ocspFailedDecodeResponse,
					err:  fmt.Errorf("failed to decode OCSP Response value in a cache. CertID: %v", encodedCertIDBase64),
				}
			}
			ocspRes, err := ocsp.ParseResponse(b, issuer)
			if err != nil {
				ocspResponseCacheLock.Lock()
				delete(ocspResponseCache, encodedCertIDBase64)
				ocspResponseCacheLock.Unlock()
				return &ocspStatus{
					code: ocspFailedParseResponse,
					err:  fmt.Errorf("failed to parse OCSP Respose. CertID: %v", encodedCertIDBase64),
				}
			}
			glog.V(2).Info("using cached OCSP Response")
			return validateOCSP(encodedCertIDBase64, ocspRes, subject)
		}
	}
	return &ocspStatus{
		code: ocspMissedCache,
		err:  fmt.Errorf("missed cache. CertID: %v", encodedCertIDBase64),
	}
}

func validateOCSP(encodedCertIDBase64 string, ocspRes *ocsp.Response, subject *x509.Certificate) *ocspStatus {
	curTime := time.Now()
	if !isInValidityRange(curTime, ocspRes.ThisUpdate, ocspRes.NextUpdate) {
		ocspResponseCacheLock.Lock()
		delete(ocspResponseCache, encodedCertIDBase64)
		ocspResponseCacheLock.Unlock()
		return &ocspStatus{
			code: ocspInvalidValidity,
			err:  fmt.Errorf("invalid validity: producedAt: %v, thisUpdate: %v, nextUpdate: %v", ocspRes.ProducedAt, ocspRes.ThisUpdate, ocspRes.NextUpdate),
		}
	}
	if ocspRes.Status != ocsp.Good {
		ocspResponseCacheLock.Lock()
		delete(ocspResponseCache, encodedCertIDBase64)
		ocspResponseCacheLock.Unlock()
		return &ocspStatus{
			code: ocspRevokedOrUnknown,
			err:  fmt.Errorf("bad revocation status. %v: %v, cert: %v", ocspRes.Status, ocspRes.RevocationReason, subject.Subject),
		}
	}
	return &ocspStatus{
		code: ocspSuccess,
		err:  nil,
	}
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
	ocspReq, err := ocsp.CreateRequest(subject, issuer, &ocsp.RequestOptions{})
	if err != nil {
		ocspStatusChan <- &ocspStatus{
			code: ocspFailedComposeRequest,
			err:  fmt.Errorf("failed to compose OCSP request object. %v", subject.Subject),
		}
		return
	}

	encodedCertID, ocspS := encodeCertID(ocspReq)
	if ocspS.code != ocspSuccess {
		ocspStatusChan <- ocspS
		return
	}

	ocspValidatedWithCache := checkOCSPResponseCache(encodedCertID, subject, issuer)
	if ocspValidatedWithCache.code == ocspSuccess {
		ocspStatusChan <- ocspValidatedWithCache
		return
	}
	glog.V(2).Infof("cached missed: %v", ocspValidatedWithCache.err)

	ocspClient := &http.Client{
		Timeout: 30 * time.Second,
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
	var ocspResBytes []byte
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
		ocspResBytes, err = ioutil.ReadAll(res.Body)
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
		ocspRes, err = ocsp.ParseResponse(ocspResBytes, issuer)
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

	encodedCertIDBase64 := base64.StdEncoding.EncodeToString(encodedCertID)
	ocspStatusChan <- validateOCSP(encodedCertIDBase64, ocspRes, subject)
	v := []interface{}{float64(time.Now().UTC().Unix()), base64.StdEncoding.EncodeToString(ocspResBytes)}
	ocspResponseCacheLock.Lock()
	ocspResponseCache[encodedCertIDBase64] = v
	ocspResponseCacheLock.Unlock()
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
	writeOCSPCacheFile()
	return nil
}

// readOCSPCacheFile reads a OCSP Response cache file. This should be called in init().
func readOCSPCacheFile() {
	ocspResponseCache = make(map[string][]interface{})
	ocspResponseCacheLock = &sync.RWMutex{}
	cacheFileName := filepath.Join(CacheDir, CacheFileName)
	raw, err := ioutil.ReadFile(cacheFileName)
	if err != nil {
		glog.V(2).Infof("failed to read OCSP cache file. %v. ignored.", err)
	}
	err = json.Unmarshal(raw, &ocspResponseCache)
	if err != nil {
		glog.V(2).Info("failed to read OCSP cache file. %v. ignored", err)
	}
}

// writeOCSPCacheFile writes a OCSP Response cache file. This is called if all revocation status is success.
// lock file is used to mitigate race condition with other process.
func writeOCSPCacheFile() {
	glog.V(2).Info("writing OCSP Response cache file")
	cacheFileName := filepath.Join(CacheDir, CacheFileName)
	cacheLockFileName := cacheFileName + ".lck"
	statinfo, err := os.Stat(cacheLockFileName)
	switch {
	case os.IsNotExist(err):
		os.OpenFile(cacheLockFileName, os.O_RDONLY|os.O_CREATE, 0644)
	case err != nil:
		glog.V(2).Infof("failed to write OCSP response cache file. file: %v, err: %v. ignored.", cacheFileName, err)
		return
	default:
		if time.Now().Sub(statinfo.ModTime()) < time.Hour {
			glog.V(2).Infof("other process locks the cache file. %v. ignored.", cacheFileName)
			return
		}
		err := os.Remove(cacheLockFileName)
		if err != nil {
			glog.V(2).Infof("failed to delete lock file. file: %v, err: %v. ignored.", cacheLockFileName, err)
			return
		}
		os.OpenFile(cacheLockFileName, os.O_RDONLY|os.O_CREATE, 0644)
	}
	defer os.Remove(cacheLockFileName)
	ocspResponseCacheLock.Lock()
	defer ocspResponseCacheLock.Unlock()
	j, err := json.Marshal(ocspResponseCache)
	if err != nil {
		glog.V(2).Info("failed to convert OCSP Response cache to JSON. ignored.")
		return
	}
	err = ioutil.WriteFile(cacheFileName, j, 0644)
	if err != nil {
		glog.V(2).Infof("failed to write OCSP Response cache. err: %v. ignored.", err)
	}
}

func init() {
	// create cache directory for OCSP response
	switch runtime.GOOS {
	case "windows":
		CacheDir = filepath.Join(os.Getenv("USERPROFILE"), "AppData", "Local", "Snowflake", "Caches")
	case "darwin":
		home := os.Getenv("HOME")
		if home == "" {
			panic("HOME is blank")
		}
		CacheDir = filepath.Join(home, "Library", "Caches", "Snowflake")
	default:
		home := os.Getenv("HOME")
		if home == "" {
			panic("HOME is blank")
		}
		CacheDir = filepath.Join(home, ".cache", "snowflake")
	}
	if _, err := os.Stat(CacheDir); os.IsNotExist(err) {
		err := os.MkdirAll(CacheDir, os.ModePerm)
		if err != nil {
			glog.V(2).Infof("failed to create cache directory. %v. ignored", err)
		}
	}
	readOCSPCacheFile()
}

// snowflakeInsecureTransport is the default tranport object that doesn't do certificate revocation check.
var snowflakeInsecureTransport = &http.Transport{
	MaxIdleConns:    10,
	IdleConnTimeout: 30 * time.Minute,
}

// snowflakeTransport includes the certificate revocation check with OCSP in parallel.
var snowflakeTransport = &http.Transport{
	TLSClientConfig: &tls.Config{
		VerifyPeerCertificate: verifyPeerCertificateParallel,
	},
	MaxIdleConns:    10,
	IdleConnTimeout: 30 * time.Minute,
}

// SnowflakeTransportSerial includes the certificate revocation check with OCSP in serial.
var SnowflakeTransportSerial = &http.Transport{
	TLSClientConfig: &tls.Config{
		VerifyPeerCertificate: verifyPeerCertificateSerial,
	},
	MaxIdleConns:    10,
	IdleConnTimeout: 30 * time.Minute,
}

// SnowflakeTransportTest includes the certificate revocation check in parallel
var SnowflakeTransportTest = snowflakeTransport
