// Example: How to connect to the server with the toml file configuration
// Prerequiste: following the Snowflake doc: https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/connecting/specify-credentials
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"

	sf "github.com/snowflakedb/gosnowflake"
)

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	os.Setenv("SNOWFLAKE_HOME", "<The directory path where the toml file exists>")
	os.Setenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME", "<DSN Name>")

	cfg, err := sf.LoadConnectionConfig()
	if err != nil {
		log.Fatalf("failed to create Config, err: %v", err)
	}
	dsn, err := sf.DSN(cfg)
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()
	query := "SELECT 1"
	rows, err := db.Query(query) // no cancel is allowed
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	defer rows.Close()
	var v int
	for rows.Next() {
		err := rows.Scan(&v)
		if err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		}
		if v != 1 {
			log.Fatalf("failed to get 1. got: %v", v)
		}
	}
	if rows.Err() != nil {
		fmt.Printf("ERROR: %v\n", rows.Err())
		return
	}
	fmt.Printf("Congrats! You have successfully run %v with Snowflake DB!\n", query)
}
