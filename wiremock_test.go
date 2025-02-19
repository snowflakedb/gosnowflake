package gosnowflake

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
)

var wiremock *wiremockClient = newWiremock()

type wiremockClient struct {
	protocol string
	host     string
	port     int
	client   http.Client
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
		protocol: wmProtocol,
		host:     wmHost,
		port:     wmPort,
	}
}

func (wm *wiremockClient) connectionConfig() *Config {
	return &Config{
		User:     "testUser",
		Host:     wm.host,
		Port:     wm.port,
		Account:  "testAccount",
		Protocol: "http",
	}
}

type wiremockMapping struct {
	filePath string
	params   map[string]string
}

func (wm *wiremockClient) registerMappings(t *testing.T, mappings ...wiremockMapping) {
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
	return fmt.Sprintf("%v://%v:%v/__admin/mappings", wm.protocol, wm.host, wm.port)
}
