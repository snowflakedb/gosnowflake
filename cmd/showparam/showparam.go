// Example: Set the session parameter in DSN and verify it
//
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
		// enable glog for Go Snowflake Driver
		flag.Parse()
	}

	// get environment variables
	env := func(k string) string {
		if value := os.Getenv(k); value != "" {
			return value
		}
		log.Fatalf("%v environment variable is not set.", k)
		return ""
	}

	account := env("SNOWFLAKE_TEST_ACCOUNT")
	user := env("SNOWFLAKE_TEST_USER")
	password := env("SNOWFLAKE_TEST_PASSWORD")

	tmfmt := "MM-DD-YYYY"
	cfg := &sf.Config{
		User:     user,
		Password: password,
		Account:  account,
		Params: map[string]*string{
			"TIMESTAMP_OUTPUT_FORMAT": &tmfmt, // session parameter
		},
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
	query := "SHOW PARAMETERS LIKE 'TIMESTAMP_OUTPUT_FORMAT'"
	rows, err := db.Query(query) // no cancel is allowed
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	defer rows.Close()
	var v1, v2, v3, v4, v5 string
	for rows.Next() {
		err := rows.Scan(&v1, &v2, &v3, &v4, &v5)
		if err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		}
		if v1 != "TIMESTAMP_OUTPUT_FORMAT" {
			log.Fatalf("failed to get 1. got: %v", v1)
		}
		fmt.Printf("fmt: %v\n", v2)
	}
	if rows.Err() != nil {
		fmt.Printf("ERROR: %v\n", rows.Err())
		return
	}
	fmt.Printf("Congrats! You have successfully run %v with Snowflake DB!\n", query)
}
