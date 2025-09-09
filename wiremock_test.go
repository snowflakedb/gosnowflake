package gosnowflake

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

var wiremock = newWiremock()
var wiremockHTTPS = newWiremockHTTPS()

type wiremockClient struct {
	protocol  string
	host      string
	port      int
	adminPort int
	client    http.Client
}

type wiremockClientHTTPS struct {
	wiremockClient
}

func newWiremock() *wiremockClient {
	wmHost := os.Getenv("WIREMOCK_HOST")
	if wmHost == "" {
		wmHost = "127.0.0.1"
	}
	wmPortStr := os.Getenv("WIREMOCK_PORT")
	if wmPortStr == "" {
		wmPortStr = "14355"
	}
	wmPort, err := strconv.Atoi(wmPortStr)
	if err != nil {
		panic(fmt.Sprintf("WIREMOCK_PORT is not a number: %v", wmPortStr))
	}
	return &wiremockClient{
		protocol:  "http",
		host:      wmHost,
		port:      wmPort,
		adminPort: wmPort,
	}
}

func newWiremockHTTPS() *wiremockClientHTTPS {
	wmHost := os.Getenv("WIREMOCK_HOST_HTTPS")
	if wmHost == "" {
		wmHost = "127.0.0.1"
	}
	wmPortStr := os.Getenv("WIREMOCK_PORT_HTTPS")
	if wmPortStr == "" {
		wmPortStr = "13567"
	}
	wmPort, err := strconv.Atoi(wmPortStr)
	if err != nil {
		panic(fmt.Sprintf("WIREMOCK_PORT is not a number: %v", wmPortStr))
	}
	wmAdminPortStr := os.Getenv("WIREMOCK_PORT")
	if wmAdminPortStr == "" {
		wmAdminPortStr = "14355"
	}
	wmAdminPort, err := strconv.Atoi(wmAdminPortStr)
	if err != nil {
		panic(fmt.Sprintf("WIREMOCK_PORT is not a number: %v", wmPortStr))
	}
	return &wiremockClientHTTPS{
		wiremockClient: wiremockClient{
			protocol:  "https",
			host:      wmHost,
			port:      wmPort,
			adminPort: wmAdminPort,
		},
	}
}

func (wm *wiremockClient) connectionConfig() *Config {
	cfg := &Config{
		Account:               "testAccount",
		User:                  "testUser",
		Password:              "testPassword",
		Host:                  wm.host,
		Port:                  wm.port,
		Protocol:              wm.protocol,
		LoginTimeout:          time.Duration(30) * time.Second,
		RequestTimeout:        time.Duration(30) * time.Second,
		MaxRetryCount:         3,
		OauthClientID:         "testClientId",
		OauthClientSecret:     "testClientSecret",
		OauthAuthorizationURL: wm.baseURL() + "/oauth/authorize",
		OauthTokenRequestURL:  wm.baseURL() + "/oauth/token",
	}
	return cfg
}

func (wm *wiremockClientHTTPS) connectionConfig(t *testing.T) *Config {
	cfg := wm.wiremockClient.connectionConfig()
	cfg.Transporter = &http.Transport{
		TLSClientConfig: wm.tlsConfig(t),
	}
	return cfg
}

func (wm *wiremockClientHTTPS) certPool(t *testing.T) *x509.CertPool {
	testCertPool := x509.NewCertPool()
	caBytes, err := os.ReadFile("ci/scripts/ca.der")
	assertNilF(t, err)
	certificate, err := x509.ParseCertificate(caBytes)
	assertNilF(t, err)
	testCertPool.AddCert(certificate)
	return testCertPool
}

func (wm *wiremockClientHTTPS) ocspTransporter(t *testing.T, delegate http.RoundTripper) http.RoundTripper {
	if delegate == nil {
		delegate = http.DefaultTransport
	}
	cfg := wm.connectionConfig(t)
	cfg.Transporter = delegate
	ov := newOcspValidator(cfg)
	return &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:               wiremockHTTPS.certPool(t),
			VerifyPeerCertificate: ov.verifyPeerCertificateSerial,
		},
		DisableKeepAlives: true,
	}
}

func (wm *wiremockClientHTTPS) tlsConfig(t *testing.T) *tls.Config {
	return &tls.Config{
		RootCAs: wm.certPool(t),
	}
}

type wiremockMapping struct {
	filePath string
	params   map[string]string
}

func newWiremockMapping(filePath string) wiremockMapping {
	return wiremockMapping{filePath: filePath}
}

func (wm *wiremockClient) registerMappings(t *testing.T, mappings ...wiremockMapping) {
	skipOnJenkins(t, "wiremock does not work on Jenkins")
	for _, mapping := range wm.enrichWithTelemetry(mappings) {
		f, err := os.Open("test_data/wiremock/mappings/" + mapping.filePath)
		assertNilF(t, err)
		defer f.Close()
		mappingBodyBytes, err := io.ReadAll(f)
		assertNilF(t, err)
		mappingBody := string(mappingBodyBytes)
		for key, val := range mapping.params {
			mappingBody = strings.Replace(mappingBody, key, val, 1)
		}
		resp, err := wm.client.Post(fmt.Sprintf("%v/import", wm.mappingsURL()), "application/json", strings.NewReader(mappingBody))
		assertNilF(t, err)
		if resp.StatusCode != http.StatusOK {
			respBody, err := io.ReadAll(resp.Body)
			assertNilF(t, err)
			t.Fatalf("cannot create mapping. status=%v body=\n%v", resp.StatusCode, string(respBody))
		}
	}
	t.Cleanup(func() {
		req, err := http.NewRequest("DELETE", wm.mappingsURL(), nil)
		assertNilE(t, err)
		_, err = wm.client.Do(req)
		assertNilE(t, err)
	})
}

func (wm *wiremockClient) enrichWithTelemetry(mappings []wiremockMapping) []wiremockMapping {
	return append(mappings, wiremockMapping{
		filePath: "telemetry.json",
	})
}

func (wm *wiremockClient) mappingsURL() string {
	return fmt.Sprintf("http://%v:%v/__admin/mappings", wm.host, wm.adminPort)
}

func (wm *wiremockClient) baseURL() string {
	return fmt.Sprintf("%v://%v:%v", wm.protocol, wm.host, wm.port)
}

func TestQueryViaHttps(t *testing.T) {
	wiremockHTTPS.registerMappings(t,
		wiremockMapping{filePath: "auth/password/successful_flow.json"},
		wiremockMapping{filePath: "select1.json", params: map[string]string{
			"%AUTHORIZATION_HEADER%": "session token",
		}},
	)
	cfg := wiremockHTTPS.connectionConfig(t)
	testCertPool := x509.NewCertPool()
	caBytes, err := os.ReadFile("ci/scripts/ca.der")
	assertNilF(t, err)
	certificate, err := x509.ParseCertificate(caBytes)
	assertNilF(t, err)
	testCertPool.AddCert(certificate)
	cfg.Transporter = &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: testCertPool,
		},
	}
	connector := NewConnector(SnowflakeDriver{}, *cfg)
	db := sql.OpenDB(connector)
	rows, err := db.Query("SELECT 1")
	assertNilF(t, err)
	defer rows.Close()
	var v int
	assertTrueF(t, rows.Next())
	assertNilF(t, rows.Scan(&v))
	assertEqualE(t, v, 1)
}
