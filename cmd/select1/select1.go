// Example: Fetch one row.
//
// No cancel is allowed as no context is specified in the method call Query(). If you want to capture Ctrl+C to cancel
// the query, specify the context and use QueryContext() instead. See selectmany for example.
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
		{"Account", "SNOWFLAKE_TEST_ACCOUNT", true},
		{"User", "SNOWFLAKE_TEST_USER", true},
		{"Password", "SNOWFLAKE_TEST_PASSWORD", true},
		{"Host", "SNOWFLAKE_TEST_HOST", false},
		{"Port", "SNOWFLAKE_TEST_PORT", false},
		{"Protocol", "SNOWFLAKE_TEST_PROTOCOL", false},
	})
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
