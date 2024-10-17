// Example: How to connect to the server with the toml file configuration
// Prerequiste: following the Snowflake doc: https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/connecting/specify-credentials
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"

	_ "github.com/snowflakedb/gosnowflake"
)

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	// os.Setenv("SNOWFLAKE_HOME", "<The directory path where the toml file exists>")
	// os.Setenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME", "<DSN Name>")

	db, err := sql.Open("snowflake", "autoConfig")
	if err != nil {
		log.Fatalf("failed to connect. %v,", err)
	}
	defer db.Close()
	query := "SELECT 1"
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	defer rows.Close()
	var v int
	if !rows.Next() {
		log.Fatalf("no rows returned, expected 1")
	}
	err = rows.Scan(&v)
	if err != nil {
		log.Fatalf("failed to get result. err: %v", err)
	}
	if v != 1 {
		log.Fatalf("failed to get 1. got: %v", v)
	}

	if rows.Err() != nil {
		fmt.Printf("ERROR: %v\n", rows.Err())
		return
	}
	fmt.Printf("Congrats! You have successfully run %v with Snowflake DB!\n", query)
}
