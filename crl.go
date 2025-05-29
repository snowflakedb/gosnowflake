package gosnowflake

import (
	"crypto/x509"
	"encoding/asn1"
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
	for _, verifiedChain := range verifiedChains {
		for certPos, cert := range verifiedChain {
			subject := string(cert.RawSubject)
			issuer := string(cert.RawIssuer)
			logger.Debugf("started CRL validation for %v", subject)
			isSelfSigned := subject == issuer
			if isSelfSigned {
				logger.Debugf("certificate %v is self signed, assuming root CA", subject)
				break
			}
			if certPos == len(verifiedChain)-1 {
				// Is it correct to assume that the last certificate in the chain is the self signed?
				return fmt.Errorf("expected last certificate to be self signed, but it's not. subject: %v, issuer: %v", subject, issuer)
			}
			issuerCert := verifiedChain[certPos+1]
			if err := cv.verifyCertificate(cert, issuerCert); err != nil {
				return err
			}
			logger.Debugf("finished CRL validation for %v", subject)
		}
	}
	return nil
}

func (cv *crlValidator) verifyCertificate(cert *x509.Certificate, issuerCert *x509.Certificate) error {
	for _, distributionPoint := range cert.CRLDistributionPoints {
		logger.Debugf("validating %v against %v", string(cert.RawSubject), distributionPoint)
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
		if string(crl.RawIssuer) != string(cert.RawIssuer) {
			logger.Debugf("failed to verify CRL issuer, got: %v, expected: %v", crl.RawIssuer, cert.RawIssuer)
			return fmt.Errorf("failed to verify CRL issuer")
		}
		if err = cv.verifyAgainstIdpExtension(crl, cert, distributionPoint); err != nil {
			return err
		}
		for _, rce := range crl.RevokedCertificateEntries {
			if cert.SerialNumber.Cmp(rce.SerialNumber) == 0 {
				logger.Warnf("certificate for %v (serial number %v) has been revoked at %v, reason: %v", string(cert.RawSubject), rce.SerialNumber, rce.RevocationTime, rce.ReasonCode)
				return fmt.Errorf("certificate for %v has been revoked", string(cert.RawSubject))
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

func (cv *crlValidator) verifyAgainstIdpExtension(crl *x509.RevocationList, cert *x509.Certificate, distributionPoint string) error {
	for _, ext := range append(crl.Extensions, crl.ExtraExtensions...) {
		if ext.Id.Equal(idpOID) {
			var idp issuingDistributionPoint
			_, err := asn1.Unmarshal(ext.Value, &idp)
			if err != nil {
				return fmt.Errorf("failed to unmarshal IDP extension: %w", err)
			}
			if string(idp.DistributionPoint.FullName[0].Bytes) != distributionPoint {
				return fmt.Errorf("distribution point %v does not match CRL IDP extension %v", distributionPoint, string(idp.DistributionPoint.FullName[0].Bytes))
			}
			return nil
		}
	}
	return nil
}
