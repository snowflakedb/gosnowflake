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
	allowCertificatesWithoutCrlURL bool
	cacheValidityTime              time.Duration
	inMemoryCacheDisabled          bool
	inMemoryCache                  map[string]*crlInMemoryCacheValueType
	inMemoryCacheMutex             sync.Mutex
	onDiskCacheDisabled            bool
	onDiskCacheDir                 string
	onDiskCacheRemovalDelay        time.Duration
	crlURLMutexes                  map[string]*sync.Mutex
	httpClient                     *http.Client
	cleanupStopChan                chan struct{}
	cleanupDoneChan                chan struct{}
}

type crlInMemoryCacheValueType struct {
	crl          *x509.RevocationList
	downloadTime *time.Time
}

func newCrlValidator(certRevocationCheckMode CertRevocationCheckMode, allowCertificatesWithoutCrlURL bool, cacheValidityTime time.Duration, inMemoryCacheDisabled, onDiskCacheDisabled bool, onDiskCacheDir string, httpClient *http.Client) *crlValidator {
	var inMemoryCache map[string]*crlInMemoryCacheValueType
	if !inMemoryCacheDisabled {
		inMemoryCache = make(map[string]*crlInMemoryCacheValueType)
	}
	return &crlValidator{
		certRevocationCheckMode:        certRevocationCheckMode,
		allowCertificatesWithoutCrlURL: allowCertificatesWithoutCrlURL,
		cacheValidityTime:              cacheValidityTime,
		inMemoryCacheDisabled:          inMemoryCacheDisabled,
		inMemoryCache:                  inMemoryCache,
		onDiskCacheDisabled:            onDiskCacheDisabled,
		onDiskCacheDir:                 onDiskCacheDir,
		onDiskCacheRemovalDelay:        7 * 24 * time.Hour, // 7 days
		crlURLMutexes:                  make(map[string]*sync.Mutex),
		httpClient:                     httpClient,
		cleanupStopChan:                make(chan struct{}),
		cleanupDoneChan:                make(chan struct{}),
	}
}

// CertRevocationCheckMode defines the modes for certificate revocation checks.
type CertRevocationCheckMode int

const (
	// CertRevocationCheckDisabled means that certificate revocation checks are disabled.
	CertRevocationCheckDisabled CertRevocationCheckMode = iota
	// CertRevocationCheckAdvisory means that certificate revocation checks are advisory, and the driver will not fail if the checks end with error (cannot verify revocation status).
	// Driver will fail only if a certicate is revoked.
	CertRevocationCheckAdvisory
	// CertRevocationCheckEnabled means that every certificate revocation check must pass, otherwise the driver will fail.
	CertRevocationCheckEnabled
)

func (m CertRevocationCheckMode) String() string {
	switch m {
	case CertRevocationCheckDisabled:
		return "CERT_REVOCATION_CHECK_DISABLED"
	case CertRevocationCheckAdvisory:
		return "CERT_REVOCATION_CHECK_ADVISORY"
	case CertRevocationCheckEnabled:
		return "CERT_REVOCATION_CHECK_ENABLED"
	default:
		return fmt.Sprintf("unknown CertRevocationCheckMode: %d", m)
	}
}

type crlValidationResult int

const (
	crlRevoked crlValidationResult = iota
	crlUnrevoked
	crlError
)

type certValidationResult int

const (
	certRevoked certValidationResult = iota
	certUnrevoked
	certError
)

// TODO in following commits:
// - clean up in memory cache and on-disk cache
// - telemetry
// - initialize into the main flow
func (cv *crlValidator) verifyPeerCertificates(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	if cv.certRevocationCheckMode == CertRevocationCheckDisabled {
		logger.Debug("certificate revocation check is disabled, skipping CRL validation")
		return nil
	}
	crlValidationResults := cv.validateChains(verifiedChains)

	allRevoked := true
	for _, result := range crlValidationResults {
		if result == crlUnrevoked {
			logger.Debug("found certificate chain with no revoked certificates")
			return nil
		}
		if result != crlRevoked {
			allRevoked = false
		}
	}

	if allRevoked {
		return fmt.Errorf("every verified certificate chain contained revoked certificates")
	}

	logger.Warn("some certificate chains didn't pass or driver wasn't able to peform the checks")
	if cv.certRevocationCheckMode == CertRevocationCheckAdvisory {
		logger.Warn("certificate revocation check is set to CERT_REVOCATION_CHECK_ADVISORY, so assuming that certificates are not revoked")
		return nil
	}
	return fmt.Errorf("certificate revocation check failed")
}

func (cv *crlValidator) validateChains(chains [][]*x509.Certificate) []crlValidationResult {
	crlValidationResults := make([]crlValidationResult, len(chains))
	for i, chain := range chains {
		crlValidationResults[i] = crlUnrevoked
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
				if cv.allowCertificatesWithoutCrlURL {
					logger.Debugf("certificate %v has no CRL distribution points, skipping CRL validation", cert.Subject)
					continue
				}
				logger.Warnf("certificate %v has no CRL distribution points, skipping CRL validation, but marking as error", cert.Subject)
				crlValidationResults[i] = crlError
				continue
			}

			certStatus := cv.validateCertificate(cert, chain[j+1])
			if certStatus == certRevoked {
				crlValidationResults[i] = crlRevoked
				break
			}

			if certStatus == certError {
				crlValidationResults[i] = crlError
				continue
			}
		}

		if crlValidationResults[i] == crlUnrevoked {
			logger.Debugf("certificate chain %d is unrevoked, skipping remaining chains", i)
			break
		}
	}

	return crlValidationResults
}

func (cv *crlValidator) validateCertificate(cert *x509.Certificate, parent *x509.Certificate) certValidationResult {
	for _, crlURL := range cert.CRLDistributionPoints {
		result := cv.validateCrlAgainstCrlURL(cert, crlURL, parent)
		if result == certRevoked || result == certError {
			return result
		}
	}
	return certUnrevoked
}

func (cv *crlValidator) validateCrlAgainstCrlURL(cert *x509.Certificate, crlURL string, parent *x509.Certificate) certValidationResult {
	now := time.Now()

	mu := cv.getOrCreateMutex(crlURL)
	mu.Lock()
	defer mu.Unlock()

	crl, downloadTime := cv.getFromCache(crlURL)
	needsFreshCrl := crl == nil || crl.NextUpdate.Before(now) || downloadTime.Add(cv.cacheValidityTime).Before(now)
	shouldUpdateCrl := false

	if needsFreshCrl {
		newCrl, newDownloadTime, err := cv.downloadCrl(crlURL)
		if err != nil {
			logger.Warnf("failed to download CRL from %v: %v", crlURL, err)
		}
		shouldUpdateCrl = newCrl != nil && (crl == nil || newCrl.ThisUpdate.After(crl.ThisUpdate))
		if shouldUpdateCrl {
			logger.Debugf("Found updated CRL for %v", crlURL)
			crl = newCrl
			downloadTime = newDownloadTime
		} else {
			if crl != nil && crl.NextUpdate.After(now) {
				logger.Debugf("CRL for %v is up-to-date, using cached version", crlURL)
			} else {
				logger.Warnf("CRL for %v is not available or outdated", crlURL)
				return certError
			}
		}
	}

	logger.Debugf("CRL has %v entries, next update at %v", len(crl.RevokedCertificateEntries), crl.NextUpdate)
	if err := cv.validateCrl(crl, parent, crlURL); err != nil {
		return certError
	}

	if shouldUpdateCrl {
		logger.Debugf("CRL for %v is valid, updating cache", crlURL)
		cv.updateCache(crlURL, crl, downloadTime)
	}

	for _, rce := range crl.RevokedCertificateEntries {
		if cert.SerialNumber.Cmp(rce.SerialNumber) == 0 {
			logger.Warnf("certificate for %v (serial number %v) has been revoked at %v, reason: %v", cert.Subject, rce.SerialNumber, rce.RevocationTime, rce.ReasonCode)
			return certRevoked
		}
	}

	return certUnrevoked
}

func (cv *crlValidator) validateCrl(crl *x509.RevocationList, parent *x509.Certificate, crlURL string) error {
	if crl.Issuer.String() != parent.Subject.String() {
		err := fmt.Errorf("CRL issuer %v does not match parent certificate subject %v for %v", crl.Issuer, parent.Subject, crlURL)
		logger.Warn(err)
		return err
	}
	if err := crl.CheckSignatureFrom(parent); err != nil {
		logger.Warnf("CRL signature verification failed for %v: %v", crlURL, err)
		return err
	}
	if err := cv.verifyAgainstIdpExtension(crl, crlURL); err != nil {
		logger.Warnf("CRL IDP extension verification failed for %v: %v", crlURL, err)
		return err
	}
	return nil
}

func (cv *crlValidator) getFromCache(crlURL string) (*x509.RevocationList, *time.Time) {
	if cv.inMemoryCacheDisabled {
		logger.Debugf("in-memory cache is disabled")
	} else {
		cv.inMemoryCacheMutex.Lock()
		cacheValue, exists := cv.inMemoryCache[crlURL]
		cv.inMemoryCacheMutex.Unlock()
		if exists {
			logger.Debugf("found CRL in cache for %v", crlURL)
			return cacheValue.crl, cacheValue.downloadTime
		}
	}
	if cv.onDiskCacheDisabled {
		logger.Debugf("CRL cache is disabled, not checking disk for %v", crlURL)
		return nil, nil
	}
	crlFilePath := cv.crlURLToPath(crlURL)
	fileHandle, err := os.Open(crlFilePath)
	if err != nil {
		logger.Debugf("cannot open CRL from disk for %v (%v): %v", crlURL, crlFilePath, err)
		return nil, nil
	}
	defer func() {
		if err := fileHandle.Close(); err != nil {
			logger.Warnf("failed to close CRL file handle for %v (%v): %v", crlURL, crlFilePath, err)
		}
	}()
	stat, err := fileHandle.Stat()
	if err != nil {
		logger.Debugf("cannot stat CRL file for %v (%v): %v", crlURL, crlFilePath, err)
		return nil, nil
	}
	crlBytes, err := io.ReadAll(fileHandle)
	if err != nil {
		logger.Debugf("cannot read CRL from disk for %v (%v): %v", crlURL, crlFilePath, err)
		return nil, nil
	}
	crl, err := x509.ParseRevocationList(crlBytes)
	if err != nil {
		logger.Warnf("cannot parse CRL from disk for %v (%v): %v", crlURL, crlFilePath, err)
		return nil, nil
	}
	modTime := stat.ModTime()

	if !cv.inMemoryCacheDisabled {
		// promote CRL to in-memory cache
		cv.inMemoryCacheMutex.Lock()
		cv.inMemoryCache[crlURL] = &crlInMemoryCacheValueType{
			crl: crl,
			// modTime is not the exact time the CRL was downloaded, but rather the last modification time of the file
			// still, it is good enough for our purposes
			downloadTime: &modTime,
		}
		cv.inMemoryCacheMutex.Unlock()
	}
	return crl, &modTime
}

func (cv *crlValidator) updateCache(crlURL string, crl *x509.RevocationList, downloadTime *time.Time) {
	if cv.inMemoryCacheDisabled {
		logger.Debugf("in-memory cache is disabled, not updating")
	} else {
		cv.inMemoryCacheMutex.Lock()
		cv.inMemoryCache[crlURL] = &crlInMemoryCacheValueType{
			crl:          crl,
			downloadTime: downloadTime,
		}
		cv.inMemoryCacheMutex.Unlock()
	}
	if cv.onDiskCacheDisabled {
		logger.Debugf("CRL cache is disabled, not writing to disk for %v", crlURL)
		return
	}
	crlFilePath := cv.crlURLToPath(crlURL)
	if err := os.MkdirAll(filepath.Dir(crlFilePath), 0755); err != nil {
		logger.Warnf("failed to create directory for CRL file %v: %v", crlFilePath, err)
		return
	}
	if err := os.WriteFile(crlFilePath, crl.Raw, 0600); err != nil {
		logger.Warnf("failed to write CRL to disk for %v (%v): %v", crlURL, crlFilePath, err)
	}
}

func (cv *crlValidator) downloadCrl(crlURL string) (*x509.RevocationList, *time.Time, error) {
	logger.Debugf("downloading CRL from %v", crlURL)
	now := time.Now()
	resp, err := cv.httpClient.Get(crlURL)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, nil, fmt.Errorf("failed to download CRL from %v, status code: %v", crlURL, resp.StatusCode)
	}
	crlBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	logger.Debugf("downloaded %v bytes for CRL %v", len(crlBytes), crlURL)
	crl, err := x509.ParseRevocationList(crlBytes)
	if err != nil {
		return nil, nil, err
	}
	return crl, &now, err
}

func (cv *crlValidator) crlURLToPath(crlURL string) string {
	// Convert CRL URL to a file path, e.g., by replacing slashes with underscores
	return filepath.Join(cv.onDiskCacheDir, url.QueryEscape(crlURL))
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

func (cv *crlValidator) getOrCreateMutex(crlURL string) *sync.Mutex {
	cv.inMemoryCacheMutex.Lock()
	mu, ok := cv.crlURLMutexes[crlURL]
	if !ok {
		mu = &sync.Mutex{}
		cv.crlURLMutexes[crlURL] = mu
	}
	cv.inMemoryCacheMutex.Unlock()
	return mu
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

func (cv *crlValidator) startPeriodicCacheCleanup(tickRate time.Duration) {
	logger.Debugf("starting periodic CRL cache cleanup with tick rate %v", tickRate)
	cv.cleanupStopChan = make(chan struct{})
	cv.cleanupDoneChan = make(chan struct{})
	go func() {
		ticker := time.NewTicker(tickRate)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				cv.cleanupInMemoryCache()
				cv.cleanupOnDiskCache()
			case <-cv.cleanupStopChan:
				close(cv.cleanupDoneChan)
				return
			}
		}
	}()
}

func (cv *crlValidator) stopPeriodicCacheCleanup() {
	logger.Debug("stopping periodic CRL cache cleanup")
	if cv.cleanupStopChan != nil {
		close(cv.cleanupStopChan)
		<-cv.cleanupDoneChan
	}
}

func (cv *crlValidator) cleanupInMemoryCache() {
	if cv.inMemoryCacheDisabled {
		return
	}
	now := time.Now()
	logger.Debugf("cleaning up in-memory CRL cache at %v", now)
	cv.inMemoryCacheMutex.Lock()
	for k, v := range cv.inMemoryCache {
		expired := v.crl.NextUpdate.Before(now)
		evicted := v.downloadTime.Add(cv.cacheValidityTime).Before(now)
		logger.Debugf("testing CRL for %v (nextUpdate=%v, downloadTime=%v) from in-memory cache (expired: %v, evicted: %v)", k, v.crl.NextUpdate, v.downloadTime, expired, evicted)
		if expired || evicted {
			delete(cv.inMemoryCache, k)
		}
	}
	cv.inMemoryCacheMutex.Unlock()
}

func (cv *crlValidator) cleanupOnDiskCache() {
	if cv.onDiskCacheDisabled {
		return
	}
	now := time.Now()
	logger.Debugf("cleaning up on-disk CRL cache at %v", now)
	entries, err := os.ReadDir(cv.onDiskCacheDir)
	if err != nil {
		logger.Warnf("failed to read CRL cache dir: %v", err)
		return
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		path := filepath.Join(cv.onDiskCacheDir, entry.Name())
		crlBytes, err := os.ReadFile(path)
		if err != nil {
			logger.Warnf("failed to read CRL file %v: %v", path, err)
			continue
		}
		crl, err := x509.ParseRevocationList(crlBytes)
		if err != nil {
			logger.Warnf("failed to parse CRL file %v: %v", path, err)
			continue
		}
		if crl.NextUpdate.Add(cv.onDiskCacheRemovalDelay).Before(now) {
			logger.Debugf("CRL file %v is expired, removing", path)
			if err := os.Remove(path); err != nil {
				logger.Warnf("failed to remove expired CRL file %v: %v", path, err)
			}
		}
	}
}
