package gosnowflake

import (
	"bytes"
	"crypto/x509"
	"encoding/asn1"
	"errors"
	"fmt"
	"io"
	"net/http"
)

var idpOID = asn1.ObjectIdentifier{2, 5, 29, 28}

type distributionPointName struct {
	FullName []asn1.RawValue `asn1:"optional,tag:0"`
}

type issuingDistributionPoint struct {
	DistributionPoint distributionPointName `asn1:"optional,tag:0"`
}

type crlValidator struct {
	httpClient http.Client
}

func newCrlValidator(httpClient http.Client) *crlValidator {
	return &crlValidator{
		httpClient: httpClient,
	}
}

// function to be set as custom TLS verification in the http client
func (cv *crlValidator) verifyPeerCertificates(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	var allErrors = errors.New("")
	for _, verifiedChain := range verifiedChains {
		isValidChain := true
		for certPos, cert := range verifiedChain {
			logger.Debugf("started CRL validation for %v", cert.Subject)
			isSelfSigned := bytes.Equal(cert.RawSubject, cert.RawIssuer)
			if isSelfSigned {
				logger.Debugf("certificate %v is self signed, assuming root CA", cert.Subject)
				break
			}
			if certPos == len(verifiedChain)-1 {
				// Is it correct to assume that the last certificate in the chain is the self signed?
				logger.Debugf("certificate %v is the last in the chain, assuming root CA", cert.Subject)
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
	for _, distributionPoint := range cert.CRLDistributionPoints {
		logger.Debugf("validating %v against %v", cert.Subject, distributionPoint)
		crlBytes, err := cv.downloadCrl(distributionPoint)
		if err != nil {
			return fmt.Errorf("failed to download CRL from %v: %w", distributionPoint, err)
		}
		crl, err := x509.ParseRevocationList(crlBytes)
		if err != nil {
			return err
		}
		logger.Debugf("parsed CRL for %v, number of revoked certificates: %v", distributionPoint, len(crl.RevokedCertificateEntries))
		if err = crl.CheckSignatureFrom(issuerCert); err != nil {
			return fmt.Errorf("signature verification error for CRL %v: %w", distributionPoint, err)
		}
		if !bytes.Equal(crl.RawIssuer, cert.RawIssuer) {
			logger.Debugf("failed to verify CRL issuer, got: %v, expected: %v", crl.Issuer, cert.Issuer)
			return fmt.Errorf("failed to verify CRL issuer")
		}
		if err = cv.verifyAgainstIdpExtension(crl, distributionPoint); err != nil {
			return err
		}
		for _, rce := range crl.RevokedCertificateEntries {
			if cert.SerialNumber.Cmp(rce.SerialNumber) == 0 {
				logger.Warnf("certificate for %v (serial number %v) has been revoked at %v, reason: %v", cert.Subject, rce.SerialNumber, rce.RevocationTime, rce.ReasonCode)
				return fmt.Errorf("certificate for %v has been revoked", cert.Subject)
			}
		}
	}
	return nil
}

func (cv *crlValidator) downloadCrl(url string) ([]byte, error) {
	logger.Debugf("downloading CRL from %v", url)
	resp, err := cv.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
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
