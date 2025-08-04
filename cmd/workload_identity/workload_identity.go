// you have to configure PAT on your user

package main

import (
	"database/sql"
	"flag"
	"fmt"
	sf "github.com/snowflakedb/gosnowflake"
	"log"
	"os"
)

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	cfg, err := sf.GetConfigFromEnv([]*sf.ConfigParam{
		{Name: "Account", EnvName: "SNOWFLAKE_TEST_ACCOUNT", FailOnMissing: true},
		{Name: "User", EnvName: "SNOWFLAKE_TEST_USER", FailOnMissing: true},
		{Name: "Host", EnvName: "SNOWFLAKE_TEST_HOST", FailOnMissing: false},
		{Name: "Port", EnvName: "SNOWFLAKE_TEST_PORT", FailOnMissing: false},
		{Name: "Protocol", EnvName: "SNOWFLAKE_TEST_PROTOCOL", FailOnMissing: false},
	})
	if err != nil {
		log.Fatalf("cannot build config. %v", err)
	}
	cfg.Authenticator = sf.AuthTypeWorkloadIdentityFederation
	identityProvider := os.Getenv("SNOWFLAKE_TEST_WORKLOAD_IDENTITY_PROVIDER")
	if identityProvider == "" {
		log.Fatalf("SNOWFLAKE_TEST_WORKLOAD_IDENTITY_PROVIDER environment variable is not set.")
	}
	cfg.WorkloadIdentityProvider = identityProvider

	connector := sf.NewConnector(sf.SnowflakeDriver{}, *cfg)
	db := sql.OpenDB(connector)
	defer db.Close()

	query := "SELECT 1"
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	defer rows.Close()
	var v int
	if !rows.Next() {
		log.Fatalf("no rows returned")
	}
	if err = rows.Scan(&v); err != nil {
		log.Fatalf("failed to scan rows. %v", err)
	}
	if v != 1 {
		log.Fatalf("unexpected result, expected 1, got %v", v)
	}
	fmt.Printf("Congrats! You have successfully run %v with Snowflake DB!\n", query)
}
