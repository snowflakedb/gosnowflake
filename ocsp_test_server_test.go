package gosnowflake

import (
	"crypto/x509"
	"golang.org/x/crypto/ocsp"
	"net/http"
	"testing"
)

type ocspTestServerType struct {
	t      *testing.T
	server *http.Server
}

func newOcspTestServer(t *testing.T) *ocspTestServerType {
	server := &http.Server{Addr: ":43578"}
	ocspTestServer := &ocspTestServerType{
		t:      t,
		server: server,
	}
	return ocspTestServer
}

func (ocspTestServer *ocspTestServerType) start() {
	go func() {
		err := ocspTestServer.server.ListenAndServe()
		assertStringContainsE(ocspTestServer.t, err.Error(), "Server closed")
	}()
}

func (ocspTestServer *ocspTestServerType) Close() error {
	return ocspTestServer.server.Close()
}

func (ocspTestServer *ocspTestServerType) ocspServerHostFunc() func(c *x509.Certificate) string {
	return func(c *x509.Certificate) string {
		return ocspTestServer.ocspServerHost()
	}
}

func (ocspTestServer *ocspTestServerType) ocspServerHost() string {
	return "http://localhost:43578"
}

func (ocspTestServer *ocspTestServerType) respondWithUnauthorized() {
	http.DefaultServeMux = http.NewServeMux()
	http.HandleFunc("/", func(resp http.ResponseWriter, request *http.Request) {
		ocspTestServer.prepareResponse(resp, ocsp.UnauthorizedErrorResponse)
	})
}

func (ocspTestServer *ocspTestServerType) respondWithMalformed() {
	http.DefaultServeMux = http.NewServeMux()
	http.HandleFunc("/", func(resp http.ResponseWriter, request *http.Request) {
		ocspTestServer.prepareResponse(resp, []byte{1, 2, 3})
	})
}

func (ocspTestServer *ocspTestServerType) prepareResponse(resp http.ResponseWriter, responseBody []byte) {
	resp.Header().Set("Content-Type", "application/ocsp-response")
	resp.Header().Set("Content-Transfer-Encoding", "Binary")
	resp.WriteHeader(http.StatusOK)
	_, err := resp.Write(responseBody)
	assertNilF(ocspTestServer.t, err)
}
