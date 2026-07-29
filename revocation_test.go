package gosnowflake

// This file holds test helpers shared by the certificate-revocation test suites
// (CRL and OCSP): a small X.509 certificate factory used to build CA and leaf
// certificates with optional CRL distribution points and OCSP responder URLs.

import (
	"cmp"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"testing"
	"time"
)

var serialNumber = int64(0) // to be incremented

// notAfterType overrides a leaf certificate's NotAfter when passed to createLeafCert.
type notAfterType time.Time

// crlEndpointType adds a CRL distribution point to a certificate created by the factory.
type crlEndpointType string

// ocspServerType adds an OCSP responder URL to a certificate created by createLeafCert.
type ocspServerType string

func createCa(t *testing.T, issuerCert *x509.Certificate, issuerPrivateKey *rsa.PrivateKey, cn string, port int, crlEndpoints ...crlEndpointType) (*rsa.PrivateKey, *x509.Certificate) {
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:       []string{"Snowflake"},
			OrganizationalUnit: []string{"Drivers"},
			Locality:           []string{"Warsaw"},
			CommonName:         cn,
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		SignatureAlgorithm:    x509.SHA256WithRSA,
	}
	return createCert(t, caTemplate, issuerCert, issuerPrivateKey, port, crlEndpoints)
}

func createLeafCert(t *testing.T, issuerCert *x509.Certificate, issuerPrivateKey *rsa.PrivateKey, port int, params ...any) (*rsa.PrivateKey, *x509.Certificate) {
	notAfter := time.Now().AddDate(1, 0, 0)
	var crlEndpoints []crlEndpointType
	var ocspServers []string
	for _, param := range params {
		switch v := param.(type) {
		case notAfterType:
			notAfter = time.Time(v)
		case crlEndpointType:
			crlEndpoints = append(crlEndpoints, v)
		case ocspServerType:
			ocspServers = append(ocspServers, string(v))
		}
	}
	serialNumber++
	certTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serialNumber),
		Subject: pkix.Name{
			Organization:       []string{"Snowflake"},
			OrganizationalUnit: []string{"Drivers"},
			Locality:           []string{"Warsaw"},
			CommonName:         "localhost",
		},
		NotBefore:          time.Now(),
		NotAfter:           notAfter,
		IsCA:               false,
		OCSPServer:         ocspServers,
		SignatureAlgorithm: x509.SHA256WithRSA,
	}
	return createCert(t, certTemplate, issuerCert, issuerPrivateKey, port, crlEndpoints)
}

func createCert(t *testing.T, template, issuerCert *x509.Certificate, issuerPrivateKey *rsa.PrivateKey, port int, crlEndpoints []crlEndpointType) (*rsa.PrivateKey, *x509.Certificate) {
	var distributionPoints []string
	for _, crlEndpoint := range crlEndpoints {
		distributionPoints = append(distributionPoints, fmt.Sprintf("http://localhost:%v%v", port, crlEndpoint))
		template.CRLDistributionPoints = distributionPoints
	}
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	assertNilF(t, err)
	template.SubjectKeyId = calculateKeyID(t, &privateKey.PublicKey)
	signerPrivateKey := cmp.Or(issuerPrivateKey, privateKey)
	issuerCertOrSelfSigned := cmp.Or(issuerCert, template)
	certBytes, err := x509.CreateCertificate(rand.Reader, template, issuerCertOrSelfSigned, &privateKey.PublicKey, signerPrivateKey)
	assertNilF(t, err)
	cert, err := x509.ParseCertificate(certBytes)
	assertNilF(t, err)
	return privateKey, cert
}

func calculateKeyID(t *testing.T, pubKey any) []byte {
	pubBytes, err := x509.MarshalPKIXPublicKey(pubKey)
	assertNilF(t, err)
	hash := sha256.Sum256(pubBytes)
	return hash[:]
}
