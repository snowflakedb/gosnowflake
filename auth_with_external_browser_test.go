package gosnowflake

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func TestExternalBrowserSuccessful(t *testing.T) {
	cfg := setupTest(t)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideCredentials(mode.Success, cfg.User, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		connectToSnowflake(cfg, "SELECT 1", true)
	}()
	wg.Wait()
}

func TestExternalBrowserFailed(t *testing.T) {
	cfg := setupTest(t)
	cfg.ExternalBrowserTimeout = time.Duration(10000) * time.Millisecond
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideCredentials(mode.Fail, "FakeAccount", "NotARealPassword")
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
	cfg := setupTest(t)
	cfg.ExternalBrowserTimeout = time.Duration(1000) * time.Millisecond
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		provideCredentials(mode.Timeout, cfg.User, cfg.Password)
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
	cfg := setupTest(t)
	correctUsername := cfg.User
	cfg.User = "fakeAccount"
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		provideCredentials(mode.Success, correctUsername, cfg.Password)
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

func setupTest(t *testing.T) *Config {
	cleanupBrowserProcesses()
	cfg, err := getConfig()
	if err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	return cfg
}

func TestClientStoreCredentials(t *testing.T) {
	cfg := setupTest(t)
	cfg.ClientStoreTemporaryCredential = 1
	cfg.ExternalBrowserTimeout = time.Duration(10000) * time.Millisecond

	t.Run("Obtains the ID token from the server and saves it on the local storage", func(t *testing.T) {
		cleanupBrowserProcesses()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			provideCredentials(mode.Success, cfg.User, cfg.Password)
		}()
		go func() {
			defer wg.Done()
			connectToSnowflake(cfg, "SELECT 1", true)
		}()
		wg.Wait()
	})

	t.Run("Verify validation of ID token if option enabled", func(t *testing.T) {
		cleanupBrowserProcesses()
		conn, _ := getConnection(openDb(cfg))
		_, err := conn.QueryContext(context.Background(), "SELECT 1")
		if err != nil {
			log.Fatalf("failed to run a query. err: %v", err)
		}
	})

	t.Run("Verify validation of IDToken if option disabled", func(t *testing.T) {
		cleanupBrowserProcesses()
		cfg.ClientStoreTemporaryCredential = 0
		tOut := "authentication timed out"
		_, err := getConnection(openDb(cfg))
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

var mode = Mode{
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

func getConfigFromEnv() (*Config, error) {
	return GetConfigFromEnv([]*ConfigParam{
		{Name: "Account", EnvName: "SNOWFLAKE_TEST_ACCOUNT", FailOnMissing: true},
		{Name: "User", EnvName: "SNOWFLAKE_AUTH_TEST_OKTA_USER", FailOnMissing: true},
		{Name: "Password", EnvName: "SNOWFLAKE_AUTH_TEST_OKTA_PASS", FailOnMissing: true},
		{Name: "Host", EnvName: "SNOWFLAKE_TEST_HOST", FailOnMissing: false},
		{Name: "Port", EnvName: "SNOWFLAKE_TEST_PORT", FailOnMissing: false},
		{Name: "Protocol", EnvName: "SNOWFLAKE_AUTH_TEST_PROTOCOL", FailOnMissing: false},
	})
}

func getConfig() (*Config, error) {
	cfg, err := getConfigFromEnv()
	if err != nil {
		return nil, err
	}

	cfg.Authenticator = AuthTypeExternalBrowser
	cfg.DisableQueryContextCache = true

	return cfg, nil
}

func parseFlags() {
	if !flag.Parsed() {
		flag.Parse()
	}
}

func executeQuery(query string, dsn string) (rows *sql.Rows, err error) {
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()

	rows, err = db.Query(query)
	return rows, err
}

func openDb(cfg *Config) *sql.DB {
	dsn, err := DSN(cfg)
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to open database. %v, err: %v", dsn, err)
	}
	return db
}

func getConnection(db *sql.DB) (*sql.Conn, error) {
	conn, err := db.Conn(context.Background())
	return conn, err
}

func connectToSnowflake(config *Config, query string, exceptionHandler bool) (rows *sql.Rows, err error) {
	parseFlags()
	cfg := config
	dsn, err := DSN(cfg)
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}
	fmt.Printf("Waiting for opening browser to authenticate...")
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
