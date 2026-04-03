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
	client := &http.Client{Timeout: 10 * time.Second}
	acct := cfg.Account
	user := cfg.User
	ver := SnowflakeGoDriverVersion

	base := fmt.Sprintf(`"CLIENT_APP_ID":"Go","CLIENT_APP_VERSION":"%s","ACCOUNT_NAME":"%s","LOGIN_NAME":"%s","AUTHENTICATOR":"EXTERNALBROWSER","BROWSER_MODE_REDIRECT_PORT":"12345"`, ver, acct, user)

	// Build the full driver payload for reference
	clientEnv := newAuthRequestClientEnvironment()
	clientEnv.Application = clientType
	driverPayload := authRequest{
		Data: authRequestData{
			ClientAppID:             clientType,
			ClientAppVersion:        SnowflakeGoDriverVersion,
			AccountName:             acct,
			LoginName:               user,
			ClientEnvironment:       clientEnv,
			Authenticator:           "EXTERNALBROWSER",
			BrowserModeRedirectPort: "12345",
		},
	}
	driverBody, _ := json.Marshal(driverPayload)

	tests := []struct {
		label string
		body  string
	}{
		// A: baseline
		{"A-minimal", fmt.Sprintf(`{"data":{%s}}`, base)},

		// B: isolate SVN_REVISION vs CLIENT_ENVIRONMENT
		{"B1-add-SVN_REVISION", fmt.Sprintf(`{"data":{%s,"SVN_REVISION":""}}`, base)},
		{"B2-add-CLIENT_ENV-empty", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{}}}`, base)},
		{"B3-add-both-empty", fmt.Sprintf(`{"data":{%s,"SVN_REVISION":"","CLIENT_ENVIRONMENT":{}}}`, base)},

		// C: CLIENT_ENVIRONMENT with individual fields
		{"C1-env-APPLICATION", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"APPLICATION":"Go"}}}`, base)},
		{"C2-env-OS", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"OS":"linux"}}}`, base)},
		{"C3-env-OS_VERSION", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"OS_VERSION":"Linux-5.4.181-99.354.amzn2.x86_64"}}}`, base)},
		{"C4-env-OS_DETAILS", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"OS_DETAILS":{"ID":"debian","NAME":"Debian GNU/Linux"}}}}`, base)},
		{"C5-env-GO_VERSION", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"GO_VERSION":"go1.25.0"}}}`, base)},
		{"C6-env-ISA", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"ISA":"amd64"}}}`, base)},
		{"C7-env-OCSP_MODE", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"OCSP_MODE":""}}}`, base)},
		{"C8-env-CORE_VERSION", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"CORE_VERSION":"0.0.1"}}}`, base)},
		{"C9-env-CORE_FILE_NAME", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"CORE_FILE_NAME":"libsf_mini_core_linux_amd64_glibc.so"}}}`, base)},
		{"C10-env-CGO_ENABLED", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"CGO_ENABLED":true}}}`, base)},
		{"C11-env-LINKING_MODE", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"LINKING_MODE":"dynamic"}}}`, base)},
		{"C12-env-LIBC_FAMILY", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"LIBC_FAMILY":"glibc"}}}`, base)},
		{"C13-env-LIBC_VERSION", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"LIBC_VERSION":"2.36"}}}`, base)},

		// D: combinations - add fields one by one to find the breaking combo
		{"D1-env-APP+OS+VER", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"APPLICATION":"Go","OS":"linux","OS_VERSION":"Linux-5.4.181-99.354.amzn2.x86_64","GO_VERSION":"go1.25.0"}}}`, base)},
		{"D2-env-all-no-core", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"APPLICATION":"Go","APPLICATION_PATH":"","OS":"linux","OS_VERSION":"Linux-5.4.181-99.354.amzn2.x86_64","OS_DETAILS":{"ID":"debian","NAME":"Debian GNU/Linux","PRETTY_NAME":"Debian GNU/Linux 12 (bookworm)","VERSION":"12 (bookworm)","VERSION_ID":"12"},"ISA":"amd64","OCSP_MODE":"","GO_VERSION":"go1.25.0"}}}`, base)},
		{"D3-env-all-with-core", fmt.Sprintf(`{"data":{%s,"CLIENT_ENVIRONMENT":{"APPLICATION":"Go","APPLICATION_PATH":"","OS":"linux","OS_VERSION":"Linux-5.4.181-99.354.amzn2.x86_64","OS_DETAILS":{"ID":"debian","NAME":"Debian GNU/Linux","PRETTY_NAME":"Debian GNU/Linux 12 (bookworm)","VERSION":"12 (bookworm)","VERSION_ID":"12"},"ISA":"amd64","OCSP_MODE":"","GO_VERSION":"go1.25.0","CORE_VERSION":"0.0.1","CORE_FILE_NAME":"libsf_mini_core_linux_amd64_glibc.so","CGO_ENABLED":true}}}`, base)},

		// E: SVN_REVISION variations
		{"E1-SVN_REVISION-empty", fmt.Sprintf(`{"data":{%s,"SVN_REVISION":""}}`, base)},
		{"E2-SVN_REVISION-value", fmt.Sprintf(`{"data":{%s,"SVN_REVISION":"12345"}}`, base)},
		{"E3-SVN_REVISION-null", fmt.Sprintf(`{"data":{%s,"SVN_REVISION":null}}`, base)},

		// F: full driver body (now with omitempty fix)
		{"F-full-driver", string(driverBody)},
	}

	t.Logf("=== DEEP PAYLOAD INVESTIGATION ===")
	for _, tt := range tests {
		body := []byte(tt.body)
		doPayloadTest(t, tt.label, client, baseURL, body)
	}
	t.Logf("=== END INVESTIGATION ===")

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
