package gosnowflake

import (
	"crypto/x509"
	"encoding/asn1"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var idpOID = asn1.ObjectIdentifier{2, 5, 29, 28}

type distributionPointName struct {
	FullName []asn1.RawValue `asn1:"optional,tag:0"`
}

type issuingDistributionPoint struct {
	DistributionPoint distributionPointName `asn1:"optional,tag:0"`
}

type crlValidator struct {
	certRevocationCheckMode        CertRevocationCheckMode
	serialCertificateValidation    bool
	allowCertificatesWithoutCrlUrl bool
	cacheValidityTime              time.Duration
	inMemoryCacheDisabled          bool
	inMemoryCache                  map[string]*crlInMemoryCacheValueType
	inMemoryCacheMutex             sync.Mutex
	onDiskCacheDisabled            bool
	onDiskCacheDir                 string
	crlUrlMus                      map[string]*sync.Mutex
	httpClient                     *http.Client
}

type crlInMemoryCacheValueType struct {
	crl          *x509.RevocationList
	downloadTime *time.Time
}

func newCrlValidator(certRevocationCheckMode CertRevocationCheckMode, serialCertificateValidation, allowCertificatesWithoutCrlUrl bool, cacheValidityTime time.Duration, inMemoryCacheDisabled, onDiskCacheDisabled bool, onDiskCacheDir string, httpClient *http.Client) *crlValidator {
	var inMemoryCache map[string]*crlInMemoryCacheValueType
	if !inMemoryCacheDisabled {
		inMemoryCache = make(map[string]*crlInMemoryCacheValueType)
	}
	return &crlValidator{
		certRevocationCheckMode:        certRevocationCheckMode,
		serialCertificateValidation:    serialCertificateValidation,
		allowCertificatesWithoutCrlUrl: allowCertificatesWithoutCrlUrl,
		cacheValidityTime:              cacheValidityTime,
		inMemoryCacheDisabled:          inMemoryCacheDisabled,
		inMemoryCache:                  inMemoryCache,
		onDiskCacheDisabled:            onDiskCacheDisabled,
		onDiskCacheDir:                 onDiskCacheDir,
		crlUrlMus:                      make(map[string]*sync.Mutex),
		httpClient:                     httpClient,
	}
}

type CertRevocationCheckMode int

const (
	CERT_REVOCATION_CHECK_DISABLED CertRevocationCheckMode = iota
	CERT_REVOCATION_CHECK_ADVISORY
	CERT_REVOCATION_CHECK_ENABLED
)

func (m CertRevocationCheckMode) String() string {
	switch m {
	case CERT_REVOCATION_CHECK_DISABLED:
		return "CERT_REVOCATION_CHECK_DISABLED"
	case CERT_REVOCATION_CHECK_ADVISORY:
		return "CERT_REVOCATION_CHECK_ADVISORY"
	case CERT_REVOCATION_CHECK_ENABLED:
		return "CERT_REVOCATION_CHECK_ENABLED"
	default:
		return fmt.Sprintf("unknown CertRevocationCheckMode: %d", m)
	}
}

type crlValidationResult int

const (
	CRL_REVOKED crlValidationResult = iota
	CRL_UNREVOKED
	CRL_ERROR
)

type certValidationResult int

const (
	CERT_REVOKED certValidationResult = iota
	CERT_UNREVOKED
	CERT_ERROR
)

// TODO in following commits:
// - clean up in memory cache and on-disk cache
// - telemetry
// - initialize into the main flow
func (cv *crlValidator) verifyPeerCertificates(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	if cv.certRevocationCheckMode == CERT_REVOCATION_CHECK_DISABLED {
		logger.Debug("certificate revocation check is disabled, skipping CRL validation")
		return nil
	}
	crlValidationResults := cv.validateChains(verifiedChains)

	for _, result := range crlValidationResults {
		if result == CRL_UNREVOKED {
			logger.Debug("found certificate chain with no revoked certificates")
			return nil
		}
	}

	allRevoked := true
	for _, result := range crlValidationResults {
		if result != CRL_REVOKED {
			allRevoked = false
			break
		}
	}
	if allRevoked {
		return fmt.Errorf("every verified certificate chain contained revoked certificates")
	}

	logger.Warn("some certificate chains didn't pass or driver wasn't able to peform the checks")
	if cv.certRevocationCheckMode == CERT_REVOCATION_CHECK_ADVISORY {
		logger.Warn("certificate revocation check is set to CERT_REVOCATION_CHECK_ADVISORY, so assuming that certificates are not revoked")
		return nil
	}
	return fmt.Errorf("certificate revocation check failed")
}

func (cv *crlValidator) validateChains(chains [][]*x509.Certificate) []crlValidationResult {
	crlValidationResults := make([]crlValidationResult, len(chains))
	for i, chain := range chains {
		crlValidationResults[i] = CRL_UNREVOKED
		chainStr := ""
		for _, cert := range chain {
			chainStr += fmt.Sprintf("%v -> ", cert.Subject)
		}
		logger.Debugf("validating certificate chain %d: %s", i, chainStr)
		for j, cert := range chain {
			if j == len(chain)-1 {
				logger.Debugf("skipping root certificate %v for CRL validation", cert.Subject)
				continue
			}

			if isShortLivedCertificate(cert) {
				logger.Debugf("certificate %v is short-lived, skipping CRL validation", cert.Subject)
				continue
			}

			if len(cert.CRLDistributionPoints) == 0 {
				if cv.allowCertificatesWithoutCrlUrl {
					logger.Debugf("certificate %v has no CRL distribution points, skipping CRL validation", cert.Subject)
					continue
				}
				logger.Warnf("certificate %v has no CRL distribution points, skipping CRL validation, but marking as error", cert.Subject)
				crlValidationResults[i] = CRL_ERROR
				continue
			}

			certStatus := cv.validateCertificate(cert, chain[j+1])
			if certStatus == CERT_REVOKED {
				crlValidationResults[i] = CRL_REVOKED
				break
			}

			if certStatus == CERT_ERROR {
				crlValidationResults[i] = CRL_ERROR
				break
			}
		}

		if crlValidationResults[i] == CRL_UNREVOKED {
			logger.Debugf("certificate chain %d is unrevoked, skipping remaining chains", i)
			break
		}
	}
	return crlValidationResults
}

func (cv *crlValidator) validateCertificate(cert *x509.Certificate, parent *x509.Certificate) certValidationResult {
	if cv.serialCertificateValidation {
		return cv.validateCertificateSerial(cert, parent)
	}

	return cv.validateCertificateInParallel(cert, parent)
}

func (cv *crlValidator) validateCertificateSerial(cert *x509.Certificate, parent *x509.Certificate) certValidationResult {
	for _, crlUrl := range cert.CRLDistributionPoints {
		result := cv.validateCrlAgainstCrlUrl(cert, crlUrl, parent)
		if result == CERT_REVOKED || result == CERT_ERROR {
			return result
		}
	}
	return CERT_UNREVOKED
}

func (cv *crlValidator) validateCertificateInParallel(cert *x509.Certificate, parent *x509.Certificate) certValidationResult {
	wg := sync.WaitGroup{}
	wg.Add(len(cert.CRLDistributionPoints))
	results := make([]certValidationResult, len(cert.CRLDistributionPoints))
	for i, crlUrl := range cert.CRLDistributionPoints {
		go func() {
			defer wg.Done()
			result := cv.validateCrlAgainstCrlUrl(cert, crlUrl, parent)
			results[i] = result
		}()
	}
	wg.Wait()
	for _, result := range results {
		if result == CERT_REVOKED || result == CERT_ERROR {
			return result
		}
	}
	return CERT_UNREVOKED
}

func (cv *crlValidator) validateCrlAgainstCrlUrl(cert *x509.Certificate, crlUrl string, parent *x509.Certificate) certValidationResult {
	now := time.Now()

	cv.inMemoryCacheMutex.Lock()
	mu, ok := cv.crlUrlMus[crlUrl]
	if !ok {
		mu = &sync.Mutex{}
		cv.crlUrlMus[crlUrl] = mu
	}
	cv.inMemoryCacheMutex.Unlock()
	mu.Lock()
	defer mu.Unlock()

	crl, downloadTime := cv.getFromCache(crlUrl)
	needsFreshCrl := crl == nil || crl.NextUpdate.Before(now) || downloadTime.Add(cv.cacheValidityTime).Before(now)
	shouldUpdateCrl := false

	if needsFreshCrl {
		newCrl, newDownloadTime, err := cv.downloadCrl(crlUrl)
		if err != nil {
			logger.Warnf("failed to download CRL from %v: %v", crlUrl, err)
		}
		shouldUpdateCrl = newCrl != nil && (crl == nil || newCrl.ThisUpdate.After(crl.ThisUpdate))
		if shouldUpdateCrl {
			logger.Debugf("Updating CRL for %v", crlUrl)
			crl = newCrl
			downloadTime = newDownloadTime
		} else {
			if crl != nil && crl.NextUpdate.Before(now) {
				logger.Debugf("CRL for %v is up-to-date, using cached version", crlUrl)
			} else {
				logger.Warnf("CRL for %v is not available or outdated", crlUrl)
				return CERT_ERROR
			}
		}
	}

	logger.Debugf("CRL has %v entries, next update at %v", len(crl.RevokedCertificateEntries), crl.NextUpdate)
	if err := cv.validateCrl(crl, parent, crlUrl); err != nil {
		return CERT_ERROR
	}

	if shouldUpdateCrl {
		logger.Debugf("CRL for %v is valid, updating cache", crlUrl)
		cv.updateCache(crlUrl, crl, downloadTime)
	}

	for _, rce := range crl.RevokedCertificateEntries {
		if cert.SerialNumber.Cmp(rce.SerialNumber) == 0 {
			logger.Warnf("certificate for %v (serial number %v) has been revoked at %v, reason: %v", cert.Subject, rce.SerialNumber, rce.RevocationTime, rce.ReasonCode)
			return CERT_REVOKED
		}
	}

	return CERT_UNREVOKED
}

func (cv *crlValidator) validateCrl(crl *x509.RevocationList, parent *x509.Certificate, crlUrl string) error {
	if crl.Issuer.String() != parent.Subject.String() {
		err := fmt.Errorf("CRL issuer %v does not match parent certificate subject %v for %v", crl.Issuer, parent.Subject, crlUrl)
		logger.Warn(err)
		return err
	}
	if err := crl.CheckSignatureFrom(parent); err != nil {
		logger.Warnf("CRL signature verification failed for %v: %v", crlUrl, err)
		return err
	}
	if err := cv.verifyAgainstIdpExtension(crl, crlUrl); err != nil {
		logger.Warnf("CRL IDP extension verification failed for %v: %v", crlUrl, err)
		return err
	}
	return nil
}

func (cv *crlValidator) getFromCache(crlUrl string) (*x509.RevocationList, *time.Time) {
	if cv.inMemoryCacheDisabled {
		logger.Debugf("in-memory cache is disabled")
	} else {
		cv.inMemoryCacheMutex.Lock()
		cacheValue, exists := cv.inMemoryCache[crlUrl]
		cv.inMemoryCacheMutex.Unlock()
		if exists {
			logger.Debugf("found CRL in cache for %v", crlUrl)
			return cacheValue.crl, cacheValue.downloadTime
		}
	}
	if cv.onDiskCacheDisabled {
		logger.Debugf("CRL cache is disabled, not checking disk for %v", crlUrl)
		return nil, nil
	}
	crlFilePath := cv.crlUrlToPath(crlUrl)
	fileHandle, err := os.Open(crlFilePath)
	if err != nil {
		logger.Debugf("cannot open CRL from disk for %v (%v): %v", crlUrl, crlFilePath, err)
		return nil, nil
	}
	defer fileHandle.Close()
	stat, err := fileHandle.Stat()
	if err != nil {
		logger.Debugf("cannot stat CRL file for %v (%v): %v", crlUrl, crlFilePath, err)
		return nil, nil
	}
	crlBytes, err := io.ReadAll(fileHandle)
	if err != nil {
		logger.Debugf("cannot read CRL from disk for %v (%v): %v", crlUrl, crlFilePath, err)
		return nil, nil
	}
	crl, err := x509.ParseRevocationList(crlBytes)
	if err != nil {
		logger.Warnf("cannot parse CRL from disk for %v (%v): %v", crlUrl, crlFilePath, err)
		return nil, nil
	}
	modTime := stat.ModTime()

	// promote CRL to in-memory cache
	cv.inMemoryCacheMutex.Lock()
	cv.inMemoryCache[crlUrl] = &crlInMemoryCacheValueType{
		crl:          crl,
		downloadTime: &modTime,
	}
	cv.inMemoryCacheMutex.Unlock()
	return crl, &modTime
}

func (cv *crlValidator) updateCache(crlUrl string, crl *x509.RevocationList, downloadTime *time.Time) {
	if cv.inMemoryCacheDisabled {
		logger.Debugf("in-memory cache is disabled, not updating")
	} else {
		cv.inMemoryCacheMutex.Lock()
		cv.inMemoryCache[crlUrl] = &crlInMemoryCacheValueType{
			crl:          crl,
			downloadTime: downloadTime,
		}
		cv.inMemoryCacheMutex.Unlock()
	}
	if cv.onDiskCacheDisabled {
		logger.Debugf("CRL cache is disabled, not writing to disk for %v", crlUrl)
		return
	}
	crlFilePath := cv.crlUrlToPath(crlUrl)
	err := os.WriteFile(crlFilePath, crl.Raw, 0600)
	if err != nil {
		logger.Warnf("failed to write CRL to disk for %v (%v): %v", crlUrl, crlFilePath, err)
	}
}

func (cv *crlValidator) downloadCrl(url string) (*x509.RevocationList, *time.Time, error) {
	logger.Debugf("downloading CRL from %v", url)
	now := time.Now()
	resp, err := cv.httpClient.Get(url)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, nil, fmt.Errorf("failed to download CRL from %v, status code: %v", url, resp.StatusCode)
	}
	crlBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	logger.Debugf("downloaded %v bytes for CRL %v", len(crlBytes), url)
	crl, err := x509.ParseRevocationList(crlBytes)
	if err != nil {
		return nil, nil, err
	}
	return crl, &now, err
}

func (cv *crlValidator) crlUrlToPath(crlUrl string) string {
	// Convert CRL URL to a file path, e.g., by replacing slashes with underscores
	return filepath.Join(cv.onDiskCacheDir, url.QueryEscape(crlUrl))
}

func (cv *crlValidator) verifyAgainstIdpExtension(crl *x509.RevocationList, distributionPoint string) error {
	for _, ext := range append(crl.Extensions, crl.ExtraExtensions...) {
		if ext.Id.Equal(idpOID) {
			var idp issuingDistributionPoint
			_, err := asn1.Unmarshal(ext.Value, &idp)
			if err != nil {
				return fmt.Errorf("failed to unmarshal IDP extension: %w", err)
			}
			for _, dp := range idp.DistributionPoint.FullName {
				if string(dp.Bytes) == distributionPoint {
					logger.Debugf("distribution point %v matches CRL IDP extension", distributionPoint)
					return nil
				}
			}
			return fmt.Errorf("distribution point %v not found in CRL IDP extension", distributionPoint)
		}
	}
	return nil
}

func isShortLivedCertificate(cert *x509.Certificate) bool {
	// https://cabforum.org/working-groups/server/baseline-requirements/requirements/
	// See Short-lived Subscriber Certificate section
	if cert.NotBefore.Before(time.Date(2024, time.March, 15, 0, 0, 0, 0, time.UTC)) {
		// Certificates issued before March 15, 2024 are not considered short-lived
		return false
	}
	maximumValidityPeriod := 7 * 24 * time.Hour
	if cert.NotBefore.Before(time.Date(2026, time.March, 15, 0, 0, 0, 0, time.UTC)) {
		maximumValidityPeriod = 10 * 24 * time.Hour
	}
	maximumValidityPeriod += time.Minute // Fix inclusion start and end time
	certValidityPeriod := cert.NotAfter.Sub(cert.NotBefore)
	return maximumValidityPeriod > certValidityPeriod
}
