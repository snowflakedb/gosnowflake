package gosnowflake

import (
	"context"
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
		provideCredentials(externalBrowserType.Success, cfg.User, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		err := connectToSnowflake(cfg, "SELECT 1", true)
		assertNilF(t, err, fmt.Sprintf("Connection failed due to %v", err))
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
		provideCredentials(externalBrowserType.Fail, "FakeAccount", "NotARealPassword")
	}()
	go func() {
		defer wg.Done()
		tOut := "authentication timed out"
		err := connectToSnowflake(cfg, "SELECT 1", false)
		assertTrueF(t, err.Error() == tOut, fmt.Sprintf("Expected %v, but got %v", tOut, err))
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
		provideCredentials(externalBrowserType.Timeout, cfg.User, cfg.Password)
	}()
	go func() {
		defer wg.Done()
		tOut := "authentication timed out"
		err := connectToSnowflake(cfg, "SELECT 1", false)
		assertTrueF(t, err.Error() == tOut, fmt.Sprintf("Expected %v, but got %v", tOut, err))
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
		err := connectToSnowflake(cfg, "SELECT 1", false)
		assertTrueF(t, err.Error() == expectedErrorMsg, fmt.Sprintf("Expected %v, but got %v", expectedErrorMsg, err))
	}()
	wg.Wait()
}

func TestClientStoreCredentials(t *testing.T) {
	cfg := setupExternalBrowserTest(t)
	cfg.ClientStoreTemporaryCredential = 1
	cfg.ExternalBrowserTimeout = time.Duration(10) * time.Second

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
			err := connectToSnowflake(cfg, "SELECT 1", true)
			assertNilF(t, err, fmt.Sprintf("Connection failed: err %v", err))
		}()
		wg.Wait()
	})

	t.Run("Verify validation of ID token if option enabled", func(t *testing.T) {
		cleanupBrowserProcesses()
		cfg.ClientStoreTemporaryCredential = 1
		conn, _ := createConnection(getDbHandler(cfg))
		_, err := conn.QueryContext(context.Background(), "SELECT 1")
		assertNilF(t, err, fmt.Sprintf("Failed to run a query. err: %v", err))
	})

	t.Run("Verify validation of IDToken if option disabled", func(t *testing.T) {
		cleanupBrowserProcesses()
		cfg.ClientStoreTemporaryCredential = 0
		tOut := "authentication timed out"
		_, err := createConnection(getDbHandler(cfg))
		assertTrueF(t, err.Error() == tOut, fmt.Sprintf("Expected %v, but got %v", tOut, err))
	})
}

type ExternalBrowserProcess struct {
	Success string
	Fail    string
	Timeout string
}

var externalBrowserType = ExternalBrowserProcess{
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

func provideCredentials(ExternalBrowserProcess string, user string, password string) {
	const provideBrowserCredentialsPath = "/externalbrowser/provideBrowserCredentials.js"
	_, err := exec.Command("node", provideBrowserCredentialsPath, ExternalBrowserProcess, user, password).Output()
	if err != nil {
		log.Fatalf("failed to execute command: %v", err)
	}
}

func connectToSnowflake(cfg *Config, query string, isCatchException bool) (err error) {
	dsn, err := DSN(cfg)
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}
	rows, err := executeQuery(query, dsn)
	if isCatchException && err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	} else if err != nil {
		return err
	}
	defer rows.Close()
	var v int
	var hasAnyRows bool
	for rows.Next() {
		hasAnyRows = true
		err := rows.Scan(&v)
		if isCatchException && err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		}
	}
	if !hasAnyRows {
		return errors.New("There were no results for query: ")
	}
	fmt.Printf("Congrats! You have successfully run '%v' with Snowflake DB! \n", query)
	return err
}

func setupExternalBrowserTest(t *testing.T) *Config {
	runOnlyOnDockerContainer(t, "Running only on Docker container")
	cleanupBrowserProcesses()
	cfg, err := getAuthTestsConfig(AuthTypeExternalBrowser)
	if err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	return cfg
}
