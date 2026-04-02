package gosnowflake

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func doPayloadTest(t *testing.T, label string, client *http.Client, targetURL string, body []byte) {
	req, err := http.NewRequest("POST", targetURL, bytes.NewReader(body))
	if err != nil {
		t.Logf("[%s] Failed to create request: %v", label, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	resp, err := client.Do(req)
	if err != nil {
		t.Logf("[%s] Request error: %v", label, err)
		return
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	t.Logf("[%s] Status=%d Proto=%s Body=%s", label, resp.StatusCode, resp.Proto, string(respBody))
}

func TestExternalBrowserSuccessful(t *testing.T) {
	cfg := setupExternalBrowserTest(t)

	baseURL := fmt.Sprintf("https://%s:%d/session/authenticator-request", cfg.Host, cfg.Port)
	urlWithRequestID := baseURL + "?requestId=" + NewUUID().String()
	client := &http.Client{Timeout: 10 * time.Second}

	// Build the exact same payload the driver sends in getIdpURLProofKey
	clientEnv := newAuthRequestClientEnvironment()
	clientEnv.Application = clientType
	driverPayload := authRequest{
		Data: authRequestData{
			ClientAppID:             clientType,
			ClientAppVersion:        SnowflakeGoDriverVersion,
			AccountName:             cfg.Account,
			LoginName:               cfg.User,
			ClientEnvironment:       clientEnv,
			Authenticator:           "EXTERNALBROWSER",
			BrowserModeRedirectPort: "12345",
		},
	}
	driverBody, _ := json.Marshal(driverPayload)

	minimalBody := []byte(fmt.Sprintf(`{"data":{"CLIENT_APP_ID":"Go","CLIENT_APP_VERSION":"%s","ACCOUNT_NAME":"%s","LOGIN_NAME":"%s","AUTHENTICATOR":"EXTERNALBROWSER","BROWSER_MODE_REDIRECT_PORT":"12345"}}`,
		SnowflakeGoDriverVersion, cfg.Account, cfg.User))

	// Payload with CLIENT_ENVIRONMENT but no extra fields
	envOnlyPayload := authRequest{
		Data: authRequestData{
			ClientAppID:             clientType,
			ClientAppVersion:        SnowflakeGoDriverVersion,
			AccountName:             cfg.Account,
			LoginName:               cfg.User,
			Authenticator:           "EXTERNALBROWSER",
			BrowserModeRedirectPort: "12345",
			ClientEnvironment: authRequestClientEnvironment{
				Application: clientType,
				Os:          "linux",
			},
		},
	}
	envOnlyBody, _ := json.Marshal(envOnlyPayload)

	t.Logf("=== PAYLOAD COMPARISON ===")
	t.Logf("Base URL: %s", baseURL)
	t.Logf("URL with requestId: %s", urlWithRequestID)
	t.Logf("Minimal body (%d bytes): %s", len(minimalBody), string(minimalBody))
	t.Logf("Env-only body (%d bytes): %s", len(envOnlyBody), string(envOnlyBody))
	t.Logf("Driver body (%d bytes): %s", len(driverBody), string(driverBody))

	// Test 1: minimal payload, no requestId
	doPayloadTest(t, "1-minimal", client, baseURL, minimalBody)
	// Test 2: minimal payload + requestId query param
	doPayloadTest(t, "2-minimal+requestId", client, urlWithRequestID, minimalBody)
	// Test 3: full driver payload, no requestId
	doPayloadTest(t, "3-driver-body", client, baseURL, driverBody)
	// Test 4: full driver payload + requestId
	doPayloadTest(t, "4-driver-body+requestId", client, urlWithRequestID, driverBody)
	// Test 5: env-only payload, no requestId
	doPayloadTest(t, "5-env-only", client, baseURL, envOnlyBody)
	// Test 6: env-only payload + requestId
	doPayloadTest(t, "6-env-only+requestId", client, urlWithRequestID, envOnlyBody)

	// Now test with the driver's actual OCSP transport
	transportFactory := newTransportFactory(cfg, &snowflakeTelemetry{})
	ocspTransport, err := transportFactory.createTransport(defaultTransportConfigs.forTransportType(transportTypeSnowflake))
	if err != nil {
		t.Logf("Failed to create OCSP transport: %v", err)
	} else {
		ocspClient := &http.Client{Timeout: 10 * time.Second, Transport: ocspTransport}
		// Test 7: full driver payload + requestId + OCSP transport (exactly like the driver)
		doPayloadTest(t, "7-driver-body+requestId+OCSP", ocspClient, urlWithRequestID, driverBody)
		// Test 8: minimal payload + OCSP transport
		doPayloadTest(t, "8-minimal+OCSP", ocspClient, baseURL, minimalBody)
	}

	// Now test with stripped account name (like DSN processing does)
	strippedAccount := cfg.Account
	if idx := len(strippedAccount); idx > 0 {
		if dotIdx := findDotInAccount(cfg.Account); dotIdx > 0 {
			strippedAccount = cfg.Account[:dotIdx]
		}
	}
	if strippedAccount != cfg.Account {
		strippedPayload := authRequest{
			Data: authRequestData{
				ClientAppID:             clientType,
				ClientAppVersion:        SnowflakeGoDriverVersion,
				AccountName:             strippedAccount,
				LoginName:               cfg.User,
				ClientEnvironment:       clientEnv,
				Authenticator:           "EXTERNALBROWSER",
				BrowserModeRedirectPort: "12345",
			},
		}
		strippedBody, _ := json.Marshal(strippedPayload)
		t.Logf("Stripped account: %s (was %s)", strippedAccount, cfg.Account)
		t.Logf("Stripped body (%d bytes): %s", len(strippedBody), string(strippedBody))
		// Test 9: driver payload with stripped account
		doPayloadTest(t, "9-stripped-account", client, baseURL, strippedBody)
		// Test 10: driver payload with stripped account + requestId + OCSP
		if ocspTransport != nil {
			ocspClient := &http.Client{Timeout: 10 * time.Second, Transport: ocspTransport}
			doPayloadTest(t, "10-stripped+requestId+OCSP", ocspClient, urlWithRequestID, strippedBody)
		}
	}

	t.Logf("=== END PAYLOAD COMPARISON ===")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideExternalBrowserCredentials(t, externalBrowserType.Success, cfg.User, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
	}()
	wg.Wait()
}

func findDotInAccount(account string) int {
	for i, c := range account {
		if c == '.' {
			return i
		}
	}
	return -1
}

func TestExternalBrowserFailed(t *testing.T) {
	cfg := setupExternalBrowserTest(t)
	cfg.ExternalBrowserTimeout = time.Duration(10) * time.Second
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideExternalBrowserCredentials(t, externalBrowserType.Fail, "FakeAccount", "NotARealPassword")
	}()
	go func() {
		defer wg.Done()
		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		assertNotNilF(t, err)
		assertEqualE(t, err.Error(), "authentication timed out")
	}()
	wg.Wait()
}

func TestExternalBrowserTimeout(t *testing.T) {
	cfg := setupExternalBrowserTest(t)
	cfg.ExternalBrowserTimeout = time.Duration(1) * time.Second
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideExternalBrowserCredentials(t, externalBrowserType.Timeout, cfg.User, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		assertNotNilF(t, err)
		assertEqualE(t, err.Error(), "authentication timed out")
	}()
	wg.Wait()
}

func TestExternalBrowserMismatchUser(t *testing.T) {
	cfg := setupExternalBrowserTest(t)
	correctUsername := cfg.User
	cfg.User = "fakeAccount"
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		provideExternalBrowserCredentials(t, externalBrowserType.Success, correctUsername, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		var snowflakeErr *SnowflakeError
		assertErrorsAsF(t, err, &snowflakeErr)
		assertEqualE(t, snowflakeErr.Number, 390191, fmt.Sprintf("Expected 390191, but got %v", snowflakeErr.Number))
	}()
	wg.Wait()
}

func TestClientStoreCredentials(t *testing.T) {
	cfg := setupExternalBrowserTest(t)
	cfg.ClientStoreTemporaryCredential = 1
	cfg.ExternalBrowserTimeout = time.Duration(10) * time.Second

	t.Run("Obtains the ID token from the server and saves it on the local storage", func(t *testing.T) {
		cleanupBrowserProcesses(t)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			provideExternalBrowserCredentials(t, externalBrowserType.Success, cfg.User, cfg.Password)
		}()
		go func() {
			defer wg.Done()
			err := verifyConnectionToSnowflakeAuthTests(t, cfg)
			assertNilE(t, err, fmt.Sprintf("Connection failed: err %v", err))
		}()
		wg.Wait()
	})

	t.Run("Verify validation of ID token if option enabled", func(t *testing.T) {
		cleanupBrowserProcesses(t)
		cfg.ClientStoreTemporaryCredential = 1
		db := getDbHandlerFromConfig(t, cfg)
		conn, err := db.Conn(context.Background())
		assertNilE(t, err, fmt.Sprintf("Failed to connect to Snowflake. err: %v", err))
		defer conn.Close()
		rows, err := conn.QueryContext(context.Background(), "SELECT 1")
		assertNilE(t, err, fmt.Sprintf("Failed to run a query. err: %v", err))
		rows.Close()
	})

	t.Run("Verify validation of idToken if option disabled", func(t *testing.T) {
		cleanupBrowserProcesses(t)
		cfg.ClientStoreTemporaryCredential = 0
		db := getDbHandlerFromConfig(t, cfg)
		_, err := db.Conn(context.Background())
		assertNotNilF(t, err)
		assertEqualE(t, err.Error(), "authentication timed out", fmt.Sprintf("Expected timeout, but got %v", err))
	})
}

type ExternalBrowserProcessResult struct {
	Success               string
	Fail                  string
	Timeout               string
	OauthOktaSuccess      string
	OauthSnowflakeSuccess string
}

var externalBrowserType = ExternalBrowserProcessResult{
	Success:               "success",
	Fail:                  "fail",
	Timeout:               "timeout",
	OauthOktaSuccess:      "externalOauthOktaSuccess",
	OauthSnowflakeSuccess: "internalOauthSnowflakeSuccess",
}

func cleanupBrowserProcesses(t *testing.T) {
	if isTestRunningInDockerContainer() {
		const cleanBrowserProcessesPath = "/externalbrowser/cleanBrowserProcesses.js"
		_, err := exec.Command("node", cleanBrowserProcessesPath).CombinedOutput()
		assertNilE(t, err, fmt.Sprintf("failed to execute command: %v", err))
	}
}

func provideExternalBrowserCredentials(t *testing.T, ExternalBrowserProcess string, user string, password string) {
	if isTestRunningInDockerContainer() {
		const provideBrowserCredentialsPath = "/externalbrowser/provideBrowserCredentials.js"
		output, err := exec.Command("node", provideBrowserCredentialsPath, ExternalBrowserProcess, user, password).CombinedOutput()
		log.Printf("Output: %s\n", output)
		assertNilE(t, err, fmt.Sprintf("failed to execute command: %v", err))
	}
}

func verifyConnectionToSnowflakeAuthTests(t *testing.T, cfg *Config) (err error) {
	dsn, err := DSN(cfg)
	assertNilE(t, err, "failed to create DSN from Config")

	db, err := sql.Open("snowflake", dsn)
	assertNilE(t, err, "failed to open Snowflake DB connection")
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		log.Printf("failed to run a query. 'SELECT 1', err: %v", err)
		return err
	}
	defer rows.Close()
	assertTrueE(t, rows.Next(), "failed to get result", "There were no results for query: ")

	return err
}

func setupExternalBrowserTest(t *testing.T) *Config {
	skipAuthTests(t, "Skipping External Browser tests")
	if err := GetLogger().SetLogLevel("debug"); err != nil {
		t.Logf("failed to set log level: %v", err)
	}
	cleanupBrowserProcesses(t)
	cfg, err := getAuthTestsConfig(t, AuthTypeExternalBrowser)
	assertNilF(t, err, fmt.Sprintf("failed to get config: %v", err))
	cfg.Tracing = "debug"
	return cfg
}
