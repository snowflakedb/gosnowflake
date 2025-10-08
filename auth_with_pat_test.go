package gosnowflake

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"
)

type PatToken struct {
	Name  string
	Value string
}

func TestEndToEndPatSuccessful(t *testing.T) {
	cfg := setupEndToEndPatTest(t)
	patToken := createEndToEndPatToken(t)
	defer removeEndToEndPatToken(t, patToken.Name)
	cfg.Token = patToken.Value
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	assertNilE(t, err, fmt.Sprintf("failed to connect. err: %v", err))
}

func TestEndToEndPatMismatchedUser(t *testing.T) {
	cfg := setupEndToEndPatTest(t)
	patToken := createEndToEndPatToken(t)
	defer removeEndToEndPatToken(t, patToken.Name)
	cfg.Token = patToken.Value
	cfg.User = "invalidUser"
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	var snowflakeErr *SnowflakeError
	assertErrorsAsF(t, err, &snowflakeErr)
	assertEqualE(t, snowflakeErr.Number, 394400, fmt.Sprintf("Expected 394400, but got %v", snowflakeErr.Number))
}

func TestEndToEndPatInvalidToken(t *testing.T) {
	cfg := setupEndToEndPatTest(t)
	cfg.Token = "invalidToken"
	err := verifyConnectionToSnowflakeAuthTests(t, cfg)
	var snowflakeErr *SnowflakeError
	assertErrorsAsF(t, err, &snowflakeErr)
	assertEqualE(t, snowflakeErr.Number, 394400, fmt.Sprintf("Expected 394400, but got %v", snowflakeErr.Number))
}

func setupEndToEndPatTest(t *testing.T) *Config {
	skipAuthTests(t, "Skipping PAT tests")
	cfg, err := getAuthTestsConfig(t, AuthTypePat)
	assertNilF(t, err, fmt.Sprintf("failed to parse: %v", err))

	return cfg

}

func getEndToEndPatSetupCommandVariables() (*Config, error) {
	return GetConfigFromEnv([]*ConfigParam{
		{Name: "User", EnvName: "SNOWFLAKE_AUTH_TEST_SNOWFLAKE_USER", FailOnMissing: true},
		{Name: "Role", EnvName: "SNOWFLAKE_AUTH_TEST_INTERNAL_OAUTH_SNOWFLAKE_ROLE", FailOnMissing: true},
	})
}

func createEndToEndPatToken(t *testing.T) *PatToken {
	cfg := setupOktaTest(t)
	patTokenName := fmt.Sprintf("PAT_GOLANG_%s", strings.ReplaceAll(time.Now().Format("20060102150405.000"), ".", ""))
	patCommandVariables, err := getEndToEndPatSetupCommandVariables()
	assertNilE(t, err, "failed to get PAT command variables")

	query := fmt.Sprintf(
		"alter user %s add programmatic access token %s ROLE_RESTRICTION = '%s' DAYS_TO_EXPIRY=1;",
		patCommandVariables.User,
		patTokenName,
		patCommandVariables.Role,
	)

	patToken, err := connectUsingOktaConnectionAndExecuteCustomCommand(t, cfg, query, true)
	assertNilE(t, err, "failed to create PAT command variables")

	return patToken

}

func removeEndToEndPatToken(t *testing.T, patTokenName string) {
	cfg := setupOktaTest(t)
	cfg.Role = "analyst"
	patCommandVariables, err := getEndToEndPatSetupCommandVariables()
	assertNilE(t, err, "failed to get PAT command variables")

	query := fmt.Sprintf(
		"alter user %s remove programmatic access token %s;",
		patCommandVariables.User,
		patTokenName,
	)

	_, err = connectUsingOktaConnectionAndExecuteCustomCommand(t, cfg, query, false)
	assertNilE(t, err, "failed to remove PAT command variables")
}

func connectUsingOktaConnectionAndExecuteCustomCommand(t *testing.T, cfg *Config, query string, returnToken bool) (*PatToken, error) {
	dsn, err := DSN(cfg)
	assertNilE(t, err, "failed to create DSN from Config")

	db, err := sql.Open("snowflake", dsn)
	assertNilE(t, err, "failed to open Snowflake DB connection")
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		log.Printf("failed to run a query: %v, err: %v", query, err)
		return nil, err

	}

	var patTokenName, patTokenValue string
	if returnToken && rows.Next() {
		if err := rows.Scan(&patTokenName, &patTokenValue); err != nil {
			t.Fatalf("failed to scan token: %v", err)
		}

		return &PatToken{Name: patTokenName, Value: patTokenValue}, nil
	}

	if returnToken {
		t.Fatalf("no results found for query: %s", query)
	}

	return nil, err
}
