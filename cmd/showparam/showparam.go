// Example: Set the session parameter in DSN and verify it
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	sf "github.com/snowflakedb/gosnowflake"
)

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	cfg, err := sf.GetConfigFromEnv([]*sf.ConfigParam{
		{Name: "Account", EnvName: "SNOWFLAKE_TEST_ACCOUNT", FailOnMissing: true},
		{Name: "User", EnvName: "SNOWFLAKE_TEST_USER", FailOnMissing: true},
		{Name: "Password", EnvName: "SNOWFLAKE_TEST_PASSWORD", FailOnMissing: true},
		{Name: "Host", EnvName: "SNOWFLAKE_TEST_HOST", FailOnMissing: false},
		{Name: "Port", EnvName: "SNOWFLAKE_TEST_PORT", FailOnMissing: false},
		{Name: "Protocol", EnvName: "SNOWFLAKE_TEST_PROTOCOL", FailOnMissing: false},
	})
	if err != nil {
		log.Fatalf("failed to create Config, err: %v", err)
	}
	tmfmt := "MM-DD-YYYY"
	cfg.Params = map[string]*string{
		"TIMESTAMP_OUTPUT_FORMAT": &tmfmt, // session parameter
	}
	dsn, err := sf.DSN(cfg)
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. err: %v", err)
	}
	defer db.Close()
	query := "SHOW PARAMETERS LIKE 'TIMESTAMP_OUTPUT_FORMAT'"
	rows, err := db.Query(query) // no cancel is allowed
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	defer rows.Close()

	for rows.Next() {
		p, err := sf.ScanSnowflakeParameter(rows)
		if err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		}
		if p.Key != "TIMESTAMP_OUTPUT_FORMAT" {
			log.Fatalf("failed to get TIMESTAMP_. got: %v", p.Value)
		}
		fmt.Printf("fmt: %v\n", p.Value)
	}
	if rows.Err() != nil {
		fmt.Printf("ERROR: %v\n", rows.Err())
		return
	}
	fmt.Printf("Congrats! You have successfully run %v with Snowflake DB!\n", query)
}
