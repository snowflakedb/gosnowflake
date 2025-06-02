package gosnowflake

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"fmt"
	"math/big"
	"net/http"
	"testing"
	"time"
)

const testCrlServerPort = 56894

var serialNumber = int64(0) // to be incremented

func TestLeafCertNotRevoked(t *testing.T) {
	caPrivateKey, caCert := createCa(t, nil, nil, "root CA", "")
	_, leafCert := createLeafCert(t, caCert, caPrivateKey, "/rootCrl")
	crl := createCrl(t, caCert, caPrivateKey)

	server := createCrlServer(t, newCrlEndpointDef("/rootCrl", crl))
	defer closeServer(t, server)

	cv := newCrlValidator(http.Client{})
	err := cv.verifyPeerCertificates(nil, [][]*x509.Certificate{{leafCert, caCert}})
	assertNilE(t, err)
}

func TestLeafCertRevoked(t *testing.T) {
	caPrivateKey, caCert := createCa(t, nil, nil, "root CA", "")
	_, leafCert := createLeafCert(t, caCert, caPrivateKey, "/rootCrl")
	crl := createCrl(t, caCert, caPrivateKey, leafCert)

	server := createCrlServer(t, newCrlEndpointDef("/rootCrl", crl))
	defer closeServer(t, server)

	cv := newCrlValidator(http.Client{})
	err := cv.verifyPeerCertificates(nil, [][]*x509.Certificate{{leafCert, caCert}})
	assertNotNilF(t, err)
	assertEqualE(t, err.Error(), "no valid certificate chain found after CRL validation. errors: certificate for CN=localhost,OU=Drivers,O=Snowflake,L=Warsaw has been revoked")
}

func TestLeafNotRevokedAndRootDoesNotProvideCrl(t *testing.T) {
	rootCaPrivateKey, rootCaCert := createCa(t, nil, nil, "root CA", "")
	intermediateCaKey, intermediateCaCert := createCa(t, rootCaCert, rootCaPrivateKey, "intermediate CA", "")
	_, leafCert := createLeafCert(t, intermediateCaCert, intermediateCaKey, "/intermediateCrl")
	intermediateCrl := createCrl(t, intermediateCaCert, intermediateCaKey)

	server := createCrlServer(t, newCrlEndpointDef("/intermediateCrl", intermediateCrl))
	defer closeServer(t, server)

	cv := newCrlValidator(http.Client{})
	err := cv.verifyPeerCertificates(nil, [][]*x509.Certificate{{leafCert, intermediateCaCert, rootCaCert}})
	assertNilE(t, err)
}

func TestIntermediateCertRevoked(t *testing.T) {
	rootCaPrivateKey, rootCaCert := createCa(t, nil, nil, "root CA", "")
	intermediateCaKey, intermediateCaCert := createCa(t, rootCaCert, rootCaPrivateKey, "intermediate CA", "/rootCrl")
	_, leafCert := createLeafCert(t, intermediateCaCert, intermediateCaKey, "")
	rootCrl := createCrl(t, rootCaCert, rootCaPrivateKey, intermediateCaCert)

	server := createCrlServer(t, newCrlEndpointDef("/rootCrl", rootCrl))
	defer closeServer(t, server)

	cv := newCrlValidator(http.Client{})
	err := cv.verifyPeerCertificates(nil, [][]*x509.Certificate{{leafCert, intermediateCaCert, rootCaCert}})
	assertEqualE(t, err.Error(), "no valid certificate chain found after CRL validation. errors: certificate for CN=intermediate CA,OU=Drivers,O=Snowflake,L=Warsaw has been revoked")
}

func TestCrlSignatureInvalid(t *testing.T) {
	caPrivateKey, caCert := createCa(t, nil, nil, "root CA", "/rootCrl")
	otherCaPrivateKey, _ := createCa(t, nil, nil, "other CA", "")
	_, leafCert := createLeafCert(t, caCert, caPrivateKey, "/rootCrl")
	crl := createCrl(t, caCert, otherCaPrivateKey) // signed with wrong key

	server := createCrlServer(t, newCrlEndpointDef("/rootCrl", crl))
	defer closeServer(t, server)

	cv := newCrlValidator(http.Client{})
	err := cv.verifyPeerCertificates(nil, [][]*x509.Certificate{{leafCert, caCert}})
	assertStringContainsE(t, err.Error(), "signature verification error")
}

func TestCrlIssuerMismatch(t *testing.T) {
	caPrivateKey, caCert := createCa(t, nil, nil, "root CA", "/rootCrl")
	otherKey, otherCert := createCa(t, nil, nil, "other CA", "")
	_, leafCert := createLeafCert(t, caCert, caPrivateKey, "/rootCrl")
	crl := createCrl(t, otherCert, otherKey) // issued by other CA

	server := createCrlServer(t, newCrlEndpointDef("/rootCrl", crl))
	defer closeServer(t, server)

	cv := newCrlValidator(http.Client{})
	err := cv.verifyPeerCertificates(nil, [][]*x509.Certificate{{leafCert, caCert}})
	assertStringContainsE(t, err.Error(), "signature verification error for CRL")
}

func TestCertWithNoCrlDistributionPoints(t *testing.T) {
	caPrivateKey, caCert := createCa(t, nil, nil, "root CA", "")
	_, leafCert := createLeafCert(t, caCert, caPrivateKey, "")

	cv := newCrlValidator(http.Client{})
	err := cv.verifyPeerCertificates(nil, [][]*x509.Certificate{{leafCert, caCert}})
	assertNilE(t, err)
}

func TestCrlDownloadFails(t *testing.T) {
	caPrivateKey, caCert := createCa(t, nil, nil, "root CA", "")
	_, leafCert := createLeafCert(t, caCert, caPrivateKey, "/rootCrl")

	server := createCrlServer(t)
	defer closeServer(t, server)

	cv := newCrlValidator(http.Client{})
	err := cv.verifyPeerCertificates(nil, [][]*x509.Certificate{{leafCert, caCert}})
	assertNotNilF(t, err)
}

func TestVerifyAgainstIdpExtensionWithDistributionPointMatch(t *testing.T) {
	caPrivateKey, caCert := createCa(t, nil, nil, "root CA", "/rootCrl")
	_, leafCert := createLeafCert(t, caCert, caPrivateKey, "/rootCrl")

	idpValue, err := asn1.Marshal(issuingDistributionPoint{
		DistributionPoint: distributionPointName{
			FullName: []asn1.RawValue{
				{Bytes: []byte(fmt.Sprintf("http://localhost:%v/rootCrl", testCrlServerPort))},
			},
		},
	})
	assertNilF(t, err)
	idpExtension := &pkix.Extension{
		Id:    idpOID,
		Value: idpValue,
	}

	crl := createCrl(t, caCert, caPrivateKey, idpExtension)

	server := createCrlServer(t, newCrlEndpointDef("/rootCrl", crl))
	defer closeServer(t, server)

	cv := newCrlValidator(http.Client{})
	err = cv.verifyPeerCertificates(nil, [][]*x509.Certificate{{leafCert, caCert}})
	assertNilE(t, err)
}

func TestVerifyAgainstIdpExtensionWithDistributionPointMismatch(t *testing.T) {
	caPrivateKey, caCert := createCa(t, nil, nil, "root CA", "/rootCrl")
	_, leafCert := createLeafCert(t, caCert, caPrivateKey, "/rootCrl")

	idpValue, err := asn1.Marshal(issuingDistributionPoint{
		DistributionPoint: distributionPointName{
			FullName: []asn1.RawValue{
				{Bytes: []byte(fmt.Sprintf("http://localhost:%v/otherCrl", testCrlServerPort))},
			},
		},
	})
	assertNilF(t, err)
	idpExtension := &pkix.Extension{
		Id:    idpOID,
		Value: idpValue,
	}

	crl := createCrl(t, caCert, caPrivateKey, idpExtension)

	server := createCrlServer(t, newCrlEndpointDef("/rootCrl", crl))
	defer closeServer(t, server)

	cv := newCrlValidator(http.Client{})
	err = cv.verifyPeerCertificates(nil, [][]*x509.Certificate{{leafCert, caCert}})
	assertNotNilF(t, err)
	assertEqualE(t, err.Error(), fmt.Sprintf("no valid certificate chain found after CRL validation. errors: distribution point http://localhost:%v/rootCrl not found in CRL IDP extension", testCrlServerPort))
}

func TestAnyValidChainCausesSuccess(t *testing.T) {
	caKey, caCert := createCa(t, nil, nil, "root CA", "/rootCrl")
	_, revokedLeaf := createLeafCert(t, caCert, caKey, "/rootCrl")
	_, validLeaf := createLeafCert(t, caCert, caKey, "/rootCrl")

	// CRL revokes only the first leaf
	crl := createCrl(t, caCert, caKey, revokedLeaf)
	server := createCrlServer(t, newCrlEndpointDef("/rootCrl", crl))
	defer closeServer(t, server)

	cv := newCrlValidator(http.Client{})
	// First chain: revoked, second chain: valid
	err := cv.verifyPeerCertificates(nil, [][]*x509.Certificate{
		{revokedLeaf, caCert},
		{validLeaf, caCert},
	})
	assertNilE(t, err)
}

func createCa(t *testing.T, issuerCert *x509.Certificate, issuerPrivateKey *rsa.PrivateKey, cn string, crlEndpoint string) (*rsa.PrivateKey, *x509.Certificate) {
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
	}
	return createCert(t, caTemplate, issuerCert, issuerPrivateKey, crlEndpoint)
}

func createLeafCert(t *testing.T, issuerCert *x509.Certificate, issuerPrivateKey *rsa.PrivateKey, crlEndpoint string) (*rsa.PrivateKey, *x509.Certificate) {
	serialNumber++
	certTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serialNumber),
		Subject: pkix.Name{
			Organization:       []string{"Snowflake"},
			OrganizationalUnit: []string{"Drivers"},
			Locality:           []string{"Warsaw"},
			CommonName:         "localhost",
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().AddDate(0, 12, 0),
		IsCA:      false,
	}
	return createCert(t, certTemplate, issuerCert, issuerPrivateKey, crlEndpoint)
}

func createCert(t *testing.T, template, issuerCert *x509.Certificate, issuerPrivateKey *rsa.PrivateKey, crlEndpoint string) (*rsa.PrivateKey, *x509.Certificate) {
	distributionPoints := []string{}
	if crlEndpoint != "" {
		distributionPoints = append(distributionPoints, fmt.Sprintf("http://localhost:%v%v", testCrlServerPort, crlEndpoint))
		template.CRLDistributionPoints = distributionPoints
	}
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	assertNilF(t, err)
	signerPrivateKey := cmp.Or(issuerPrivateKey, privateKey)
	issuerCertOrSelfSigned := cmp.Or(issuerCert, template)
	certBytes, err := x509.CreateCertificate(rand.Reader, template, issuerCertOrSelfSigned, &privateKey.PublicKey, signerPrivateKey)
	assertNilF(t, err)
	cert, err := x509.ParseCertificate(certBytes)
	assertNilF(t, err)
	return privateKey, cert
}

func createCrl(t *testing.T, issuerCert *x509.Certificate, issuerPrivateKey *rsa.PrivateKey, args ...any) *x509.RevocationList {
	var revokedCertEntries []x509.RevocationListEntry
	var extensions []pkix.Extension
	for _, arg := range args {
		switch v := arg.(type) {
		case *x509.Certificate:
			revokedCertEntries = append(revokedCertEntries, x509.RevocationListEntry{
				SerialNumber:   v.SerialNumber,
				RevocationTime: time.Now().Add(-time.Hour * 24),
			})
		case *pkix.Extension:
			extensions = append(extensions, *v)
		}
	}
	crlTemplate := &x509.RevocationList{
		Number:                    big.NewInt(1),
		RevokedCertificateEntries: revokedCertEntries,
		ExtraExtensions:           extensions,
	}
	crlBytes, err := x509.CreateRevocationList(rand.Reader, crlTemplate, issuerCert, issuerPrivateKey)
	assertNilF(t, err)
	crl, err := x509.ParseRevocationList(crlBytes)
	assertNilF(t, err)
	return crl
}

type crlEndpointDef struct {
	endpoint string
	crl      *x509.RevocationList
}

func newCrlEndpointDef(endpoint string, crl *x509.RevocationList) *crlEndpointDef {
	return &crlEndpointDef{
		endpoint: endpoint,
		crl:      crl,
	}
}

func createCrlServer(t *testing.T, endpointDefs ...*crlEndpointDef) *http.Server {
	mux := http.NewServeMux()
	for _, endpointDef := range endpointDefs {
		mux.HandleFunc(endpointDef.endpoint, func(responseWriter http.ResponseWriter, request *http.Request) {
			responseWriter.WriteHeader(http.StatusOK)
			_, err := responseWriter.Write(endpointDef.crl.Raw)
			assertNilF(t, err)
		})
	}
	server := &http.Server{
		Addr:    fmt.Sprintf(":%v", testCrlServerPort),
		Handler: mux,
	}
	go func() {
		err := server.ListenAndServe()
		assertErrIsF(t, err, http.ErrServerClosed)
	}()
	return server
}

func closeServer(t *testing.T, server *http.Server) {
	err := server.Shutdown(context.Background())
	assertNilF(t, err)
}
