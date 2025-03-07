package gosnowflake

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
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
	wmProtocol := os.Getenv("WIREMOCK_PROTOCOL")
	if wmProtocol == "" {
		wmProtocol = "http"
	}
	return &wiremockClient{
		protocol:  wmProtocol,
		host:      wmHost,
		port:      wmPort,
		adminPort: wmPort,
	}
}

func newWiremockHTTPS() *wiremockClient {
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
	wmProtocol := os.Getenv("WIREMOCK_PROTOCOL")
	if wmProtocol == "" {
		wmProtocol = "https"
	}
	return &wiremockClient{
		protocol:  wmProtocol,
		host:      wmHost,
		port:      wmPort,
		adminPort: wmAdminPort,
	}
}

func (wm *wiremockClient) connectionConfig() *Config {
	cfg := &Config{
		User:     "testUser",
		Password: "testPassword",
		Host:     wm.host,
		Port:     wm.port,
		Account:  "testAccount",
		Protocol: wm.protocol,
	}
	if wm.protocol == "https" {
		testCertPool := x509.NewCertPool()
		caBytes, err := os.ReadFile("ci/scripts/ca.der")
		if err != nil {
			log.Fatalf("cannot read CA cert file. %v", err)
		}
		certificate, err := x509.ParseCertificate(caBytes)
		if err != nil {
			log.Fatalf("cannot parse certifacte. %v", err)
		}
		testCertPool.AddCert(certificate)
		cfg.Transporter = &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: testCertPool,
			},
		}
	}
	return cfg
}

type wiremockMapping struct {
	filePath string
	params   map[string]string
}

func (wm *wiremockClient) registerMappings(t *testing.T, mappings ...wiremockMapping) {
	skipOnJenkins(t, "wiremock is not enabled on Jenkins")

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
			t.Fatalf("cannot create mapping.\n%v", string(respBody))
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

func TestQueryViaHttps(t *testing.T) {
	wiremockHTTPS.registerMappings(t,
		wiremockMapping{filePath: "auth/password/successful_flow.json"},
		wiremockMapping{filePath: "select1.json", params: map[string]string{
			"%AUTHORIZATION_HEADER%": "session token",
		}},
	)
	cfg := wiremockHTTPS.connectionConfig()
	testCertPool := x509.NewCertPool()
	caBytes, err := os.ReadFile("ci/scripts/ca.der")
	assertNilF(t, err)
	certificate, err := x509.ParseCertificate(caBytes)
	assertNilF(t, err)
	testCertPool.AddCert(certificate)
	cfg.Transporter = &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:               testCertPool,
			VerifyPeerCertificate: verifyPeerCertificateSerial,
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
