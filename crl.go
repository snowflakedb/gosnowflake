package gosnowflake

import (
	"bytes"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
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
	failClosed      bool
	httpClient      *http.Client
	enableDiskCache bool
	cacheDir        string
	diskCacheTTL    time.Duration
	inMemoryCachingEnabled  bool
	revocationStatusesCache map[revocationStatusCacheKey]revocationStatusCacheValue
	inMemoryCacheTTL        time.Duration
	// TODO clear cache after some time
}

type revocationStatusCacheKey struct {
	issuerName       string
	skidBase64       string
	serialNumberBase string
}

type revocationStatusCacheValue struct {
	revoked   bool
	checkedAt *time.Time
}

func newCrlValidator(failClosed bool, httpClient *http.Client, cacheDir string) *crlValidator {
	return &crlValidator{
		failClosed:      failClosed,
		httpClient:      httpClient,
		enableDiskCache: true,
		cacheDir:        cacheDir,
		diskCacheTTL:    4 * time.Hour,
		inMemoryCachingEnabled:  true,
		revocationStatusesCache: make(map[revocationStatusCacheKey]revocationStatusCacheValue),
		inMemoryCacheTTL:        4 * time.Hour,
	}
}

// function to be set as custom TLS verification in the http client
func (cv *crlValidator) verifyPeerCertificates(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	var allErrors = errors.New("")
	for _, verifiedChain := range verifiedChains {
		isValidChain := true
		for certPos, cert := range verifiedChain {
			logger.Debugf("started CRL validation for %v", cert.Subject)
			if certPos == len(verifiedChain)-1 {
				// Is it correct to assume that the last certificate in the chain is the self signed?
				logger.Debugf("last certificate in chain: %v", cert.Subject)
				break
			}
			issuerCert := verifiedChain[certPos+1]
			if err := cv.verifyCertificate(cert, issuerCert); err != nil {
				logger.Debugf("CRL validation failed for %v: %v", cert.Subject, err)
				allErrors = fmt.Errorf("%w%w", allErrors, err)
				isValidChain = false
				break
			}
			logger.Debugf("finished CRL validation for %v", cert.Subject)
		}
		if isValidChain {
			return nil
		}
	}
	return fmt.Errorf("no valid certificate chain found after CRL validation. errors: %w", allErrors)
}

func (cv *crlValidator) verifyCertificate(cert *x509.Certificate, issuerCert *x509.Certificate) error {
	cacheKey := revocationStatusCacheKey{
		issuerName:       cert.Issuer.String(),
		skidBase64:       base64.StdEncoding.EncodeToString(cert.SubjectKeyId),
		serialNumberBase: base64.StdEncoding.EncodeToString(cert.SerialNumber.Bytes()),
	}
	if cv.inMemoryCachingEnabled {
		if cachedStatus, found := cv.revocationStatusesCache[cacheKey]; found {
			if cachedStatus.checkedAt.Add(cv.inMemoryCacheTTL).Before(time.Now()) {
				delete(cv.revocationStatusesCache, cacheKey)
			} else {
				if cachedStatus.revoked {
					return fmt.Errorf("certificate for %v has been revoked based on in memory cache (checked at %v)", cert.Subject, cachedStatus.checkedAt)
				}
				logger.Debugf("certificate for %v is not revoked based on in memory cache (checked at %v)", cert.Subject, cachedStatus.checkedAt)
				return nil
			}
		}
	}
	if len(cert.CRLDistributionPoints) == 0 {
		if cv.failClosed {
			return fmt.Errorf("certificate %v has no CRL distribution points", cert.Subject)
		}
		logger.Warnf("certificate %v has no CRL distribution points, skipping CRL validation", cert.Subject)
		return nil
	}
	for _, distributionPoint := range cert.CRLDistributionPoints {
		crl, err := cv.getCrlForDistributionPoint(cert, issuerCert, distributionPoint)
		if err != nil {
			err = fmt.Errorf("failed to get CRL for %v: %w", distributionPoint, err)
			if cv.failClosed {
				return err
			}
			logger.Warn(err)
			continue
		}
		for _, rce := range crl.RevokedCertificateEntries {
			if cert.SerialNumber.Cmp(rce.SerialNumber) == 0 {
				if cv.inMemoryCachingEnabled {
					now := time.Now()
					cv.revocationStatusesCache[cacheKey] = revocationStatusCacheValue{
						revoked:   true,
						checkedAt: &now,
					}
				}
				logger.Warnf("certificate for %v (serial number %v) has been revoked at %v, reason: %v", cert.Subject, rce.SerialNumber, rce.RevocationTime, rce.ReasonCode)
				return fmt.Errorf("certificate for %v has been revoked", cert.Subject)
			}
		}
	}
	if cv.inMemoryCachingEnabled {
		now := time.Now()
		cv.revocationStatusesCache[cacheKey] = revocationStatusCacheValue{
			revoked:   false,
			checkedAt: &now,
		}
	}
	return nil
}

func (cv *crlValidator) getCrlForDistributionPoint(cert *x509.Certificate, issuerCert *x509.Certificate, distributionPoint string) (*x509.RevocationList, error) {
	logger.Debugf("validating %v against %v", cert.Subject, distributionPoint)
	crlBytes, downloadTime, err := cv.getCrlFromDisk(distributionPoint)
	if err != nil {
		logger.Debugf("failed to read CRL %v from disk: %v", distributionPoint, err)
	}
	freshDownload := false
	var crl *x509.RevocationList
	if crlBytes != nil {
		crl, err = x509.ParseRevocationList(crlBytes)
		if err != nil {
			logger.Debugf("failed to parse CRL for %v from disk cache: %v", distributionPoint, err)
		}
	}
	// If the CRL is not found in the disk cache or is outdated, download it
	if downloadTime == nil || downloadTime.Add(cv.diskCacheTTL).Before(time.Now()) || (crl != nil && crl.NextUpdate.Before(time.Now())) {
		logger.Debugf("CRL for %v is outdated by disk TTL or missing, downloading", distributionPoint)
		if crlBytes, err = cv.downloadCrl(distributionPoint); err != nil {
			err = fmt.Errorf("failed to download CRL from %v: %w", distributionPoint, err)
			if cv.failClosed { // if fail open, we can continue without fresh CRL
				return nil, err
			}
			logger.Debug(err)
		} else {
			freshDownload = true
		}
	}
	if freshDownload {
		newCrl, err := x509.ParseRevocationList(crlBytes)
		if err != nil {
			err = fmt.Errorf("failed to parse CRL for %v: %w", distributionPoint, err)
			if cv.failClosed { // if fail open, we can continue without fresh CRL
				return nil, err
			}
			logger.Debug(err)
		} else {
			if newCrl.NextUpdate.Before(time.Now()) {
				err = fmt.Errorf("nextUpdate is in the past for %v, CRL was not updated or some reply attack? ", distributionPoint)
				if cv.failClosed { // if fail open, we can continue without fresh CRL
					return nil, err
				}
				logger.Debug(err)
			} else {
				if crl == nil || newCrl.Number.Cmp(crl.Number) > 0 {
					logger.Debugf("CRL for %v is newer than the one in cache, using the downloaded one", distributionPoint)
					crl = newCrl
				} else if newCrl.Number.Cmp(crl.Number) == 0 {
					logger.Debugf("CRL number for %v is the same as the one in cache, reusing old CRL", distributionPoint)
				} else {
					err = fmt.Errorf("downloaded CRL for %v is older than the one in cache, reply attack? ", distributionPoint)
					logger.Warn(err)
					if cv.failClosed {
						return nil, err
					}
					freshDownload = false
				}
			}
		}
	}
	if crl == nil {
		return nil, fmt.Errorf("failed to obtain CRL for %v", distributionPoint)
	}

	logger.Debugf("parsed CRL for %v, number of revoked certificates: %v", distributionPoint, len(crl.RevokedCertificateEntries))
	if err = crl.CheckSignatureFrom(issuerCert); err != nil {
		return nil, fmt.Errorf("signature verification error for CRL %v: %w", distributionPoint, err)
	}
	if !bytes.Equal(crl.RawIssuer, cert.RawIssuer) {
		logger.Warnf("failed to verify CRL issuer, got: %v, expected: %v", crl.Issuer, cert.Issuer)
		return nil, fmt.Errorf("failed to verify CRL issuer")
	}
	if err = cv.verifyAgainstIdpExtension(crl, distributionPoint); err != nil {
		logger.Warnf("failed to verify against IDP extension for CRL %v: %v", distributionPoint, err)
		return nil, err
	}
	if freshDownload && cv.enableDiskCache {
		if err = cv.saveCrlToDisk(distributionPoint, crl); err != nil {
			logger.Warnf("failed to save CRL to disk: %v", err)
		}
	}
	return crl, nil
}

func (cv *crlValidator) downloadCrl(url string) ([]byte, error) {
	logger.Debugf("downloading CRL from %v", url)
	resp, err := cv.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("status code: %v", resp.StatusCode)
	}
	crlBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	logger.Debugf("downloaded %v bytes for CRL %v", len(crlBytes), url)
	return crlBytes, err
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

func (cv *crlValidator) cacheFileName(distributionPoint string) string {
	return base64.URLEncoding.EncodeToString([]byte(distributionPoint))
}

func (cv *crlValidator) saveCrlToDisk(distributionPoint string, crl *x509.RevocationList) error {
	return os.WriteFile(filepath.Join(cv.cacheDir, cv.cacheFileName(distributionPoint)), crl.Raw, 0644)
}

func (cv *crlValidator) getCrlFromDisk(distributionPoint string) ([]byte, *time.Time, error) {
	file, err := os.OpenFile(filepath.Join(cv.cacheDir, cv.cacheFileName(distributionPoint)), os.O_RDONLY, 0644)
	if errors.Is(err, os.ErrNotExist) {
		logger.Debugf("CRL cache file %v does not exist", distributionPoint)
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open CRL cache file %v: %w", distributionPoint, err)
	}
	defer file.Close()
	crlBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read CRL cache file %v: %w", distributionPoint, err)
	}
	logger.Debugf("read %v bytes from CRL cache file %v", len(crlBytes), distributionPoint)
	stat, err := file.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get CRL cache file %v stat: %w", distributionPoint, err)
	}
	downloadTime := stat.ModTime()
	logger.Debugf("CRL cache file %v last modified at %v", distributionPoint, downloadTime)
	return crlBytes, &downloadTime, nil
}
