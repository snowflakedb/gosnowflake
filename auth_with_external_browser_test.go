package gosnowflake

import (
	"context"
	"database/sql"
	"errors"
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
		provideExternalBrowserCredentials(t, externalBrowserType.Success, cfg.User, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		err := verifyConnectionToSnowflakeAuthTests(t, cfg)
		assertNilE(t, err, fmt.Sprintf("Connection failed due to %v", err))
	}()
	wg.Wait()
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
		assertTrueF(t, errors.As(err, &snowflakeErr))
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

	t.Run("Verify validation of IDToken if option disabled", func(t *testing.T) {
		cleanupBrowserProcesses(t)
		cfg.ClientStoreTemporaryCredential = 0
		db := getDbHandlerFromConfig(t, cfg)
		_, err := db.Conn(context.Background())
		assertEqualE(t, err.Error(), "authentication timed out", fmt.Sprintf("Expected timeout, but got %v", err))
	})
}

type ExternalBrowserProcessResult struct {
	Success string
	Fail    string
	Timeout string
}

var externalBrowserType = ExternalBrowserProcessResult{
	Success: "success",
	Fail:    "fail",
	Timeout: "timeout",
}

func cleanupBrowserProcesses(t *testing.T) {
	const cleanBrowserProcessesPath = "/externalbrowser/cleanBrowserProcesses.js"
	_, err := exec.Command("node", cleanBrowserProcessesPath).Output()
	assertNilE(t, err, fmt.Sprintf("failed to execute command: %v", err))
}

func provideExternalBrowserCredentials(t *testing.T, ExternalBrowserProcess string, user string, password string) {
	const provideBrowserCredentialsPath = "/externalbrowser/provideBrowserCredentials.js"
	_, err := exec.Command("node", provideBrowserCredentialsPath, ExternalBrowserProcess, user, password).Output()
	assertNilE(t, err, fmt.Sprintf("failed to execute command: %v", err))
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
	runOnlyOnDockerContainer(t, "Running only on Docker container")
	cleanupBrowserProcesses(t)
	cfg, err := getAuthTestsConfig(t, AuthTypeExternalBrowser)
	assertNilF(t, err, fmt.Sprintf("failed to get config: %v", err))
	return cfg
}
