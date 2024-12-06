package gosnowflake

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func TestExternalBrowserSuccessful(t *testing.T) {
	cfg := setupExternalBrowserTest(t)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideCredentials(externalBrowserType.Success, cfg.User, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		_, err := connectToSnowflake(cfg, "SELECT 1", true)
		if err != nil {
			t.Errorf("Connection failed: err %v", err)
		}
	}()
	wg.Wait()
}

func TestExternalBrowserFailed(t *testing.T) {
	cfg := setupExternalBrowserTest(t)
	cfg.ExternalBrowserTimeout = time.Duration(10000) * time.Millisecond
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideCredentials(externalBrowserType.Fail, "FakeAccount", "NotARealPassword")
	}()
	go func() {
		defer wg.Done()
		tOut := "authentication timed out"
		_, err := connectToSnowflake(cfg, "SELECT 1", false)
		if err.Error() != tOut {
			t.Errorf("Expected %v, but got %v", tOut, err)
		}
	}()
	wg.Wait()
}

func TestExternalBrowserTimeout(t *testing.T) {
	cfg := setupExternalBrowserTest(t)
	cfg.ExternalBrowserTimeout = time.Duration(1000) * time.Millisecond
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideCredentials(externalBrowserType.Timeout, cfg.User, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		tOut := "authentication timed out"
		_, err := connectToSnowflake(cfg, "SELECT 1", false)
		if err.Error() != tOut {
			t.Errorf("Expected %v, but got %v", tOut, err)
		}
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
		provideCredentials(externalBrowserType.Success, correctUsername, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		expectedErrorMsg := "390191 (08004): The user you were trying to authenticate " +
			"as differs from the user currently logged in at the IDP."

		_, err := connectToSnowflake(cfg, "SELECT 1", false)
		if err.Error() != expectedErrorMsg {
			t.Errorf("Expected %v, but got %v", expectedErrorMsg, err)
		}
	}()
	wg.Wait()
}

func TestClientStoreCredentials(t *testing.T) {
	cfg := setupExternalBrowserTest(t)
	cfg.ClientStoreTemporaryCredential = 1
	cfg.ExternalBrowserTimeout = time.Duration(10000) * time.Millisecond

	t.Run("Obtains the ID token from the server and saves it on the local storage", func(t *testing.T) {
		cleanupBrowserProcesses()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			provideCredentials(externalBrowserType.Success, cfg.User, cfg.Password)
		}()
		go func() {
			defer wg.Done()
			conn, err := connectToSnowflake(cfg, "SELECT 1", true)
			if err != nil {
				t.Errorf("Connection failed: err %v", err)
			}
			defer conn.Close()
		}()
		wg.Wait()
	})

	t.Run("Verify validation of ID token if option enabled", func(t *testing.T) {
		cleanupBrowserProcesses()
		cfg.ClientStoreTemporaryCredential = 1
		conn, _ := createConnection(getDbHandler(cfg))
		_, err := conn.QueryContext(context.Background(), "SELECT 1")
		if err != nil {
			log.Fatalf("failed to run a query. err: %v", err)
		}
	})

	t.Run("Verify validation of IDToken if option disabled", func(t *testing.T) {
		cleanupBrowserProcesses()
		cfg.ClientStoreTemporaryCredential = 0
		tOut := "authentication timed out"
		_, err := createConnection(getDbHandler(cfg))
		if err.Error() != tOut {
			t.Errorf("Expected %v, but got %v", tOut, err)
		}
	})
}

type Mode struct {
	Success string
	Fail    string
	Timeout string
}

var externalBrowserType = Mode{
	Success: "success",
	Fail:    "fail",
	Timeout: "timeout",
}

func cleanupBrowserProcesses() {
	const cleanBrowserProcessesPath = "/externalbrowser/cleanBrowserProcesses.js"
	_, err := exec.Command("node", cleanBrowserProcessesPath).Output()
	if err != nil {
		log.Fatalf("failed to execute command: %v", err)
	}
}

func provideCredentials(mode string, user string, password string) {
	const provideBrowserCredentialsPath = "/externalbrowser/provideBrowserCredentials.js"
	_, err := exec.Command("node", provideBrowserCredentialsPath, mode, user, password).Output()
	if err != nil {
		log.Fatalf("failed to execute command: %v", err)
	}
}

func connectToSnowflake(cfg *Config, query string, exceptionHandler bool) (rows *sql.Rows, err error) {
	parseFlags()
	dsn, err := DSN(cfg)
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}
	rows, err = executeQuery(query, dsn)
	if exceptionHandler && err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", rows, err)
	} else if err != nil {
		return rows, err
	}
	defer rows.Close()
	var v int
	for rows.Next() {
		err := rows.Scan(&v)
		if exceptionHandler && err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		} else if exceptionHandler {
			fmt.Printf("Congrats! You have successfully run '%v' with Snowflake DB! \n", query)
		}
	}
	return rows, err
}

func setupExternalBrowserTest(t *testing.T) *Config {
	skipOnJenkins(t, "Running only on Docker container")
	if runningOnGithubAction() {
		t.Skip("Running only on Docker container")
	}
	cleanupBrowserProcesses()
	cfg, err := getConfig(AuthTypeExternalBrowser)
	if err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	return cfg
}
