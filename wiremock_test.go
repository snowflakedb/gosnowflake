package gosnowflake

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

var wiremock *wiremockClient = newWiremock()

type wiremockClient struct {
	host   string
	port   int
	client http.Client
}

func newWiremock() *wiremockClient {
	return &wiremockClient{
		host: "127.0.0.1",
		port: 14355,
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
	return fmt.Sprintf("http://%v:%v/__admin/mappings", wm.host, wm.port)
}

// just to satisfy not used private variables and functions
// to be removed with first real PR that uses wiremock
func TestWiremock(t *testing.T) {
	skipOnJenkins(t, "wiremock is not enabled on Jenkins")
	wiremock.registerMappings(t,
		wiremockMapping{filePath: "select1.json"})
	wiremock.connectionConfig()
}
