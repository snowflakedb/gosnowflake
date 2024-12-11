package gosnowflake

import (
	"context"
	"database/sql"
	"flag"
	"log"
)

func getConfigFromEnv() (*Config, error) {
	return GetConfigFromEnv([]*ConfigParam{
		{Name: "Account", EnvName: "SNOWFLAKE_TEST_ACCOUNT", FailOnMissing: true},
		{Name: "User", EnvName: "SNOWFLAKE_AUTH_TEST_OKTA_USER", FailOnMissing: true},
		{Name: "Password", EnvName: "SNOWFLAKE_AUTH_TEST_OKTA_PASS", FailOnMissing: true},
		{Name: "Host", EnvName: "SNOWFLAKE_TEST_HOST", FailOnMissing: false},
		{Name: "Port", EnvName: "SNOWFLAKE_TEST_PORT", FailOnMissing: false},
		{Name: "Protocol", EnvName: "SNOWFLAKE_AUTH_TEST_PROTOCOL", FailOnMissing: false},
		{Name: "Role", EnvName: "SNOWFLAKE_TEST_ROLE", FailOnMissing: false},
	})
}

func getConfig(authMethod AuthType) (*Config, error) {
	cfg, err := getConfigFromEnv()
	if err != nil {
		return nil, err
	}

	cfg.Authenticator = authMethod
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

func getDbHandler(cfg *Config) *sql.DB {
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

func createConnection(db *sql.DB) (*sql.Conn, error) {
	conn, err := db.Conn(context.Background())
	return conn, err
}
