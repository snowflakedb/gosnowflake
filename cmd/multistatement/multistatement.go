package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	sf "github.com/snowflakedb/gosnowflake"
)

func getDSN() (string, *sf.Config, error) {
	env := func(key string, failOnMissing bool) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		if failOnMissing {
			log.Fatalf("#{key} environment not set")
		}
		return ""
	}

	account := env("SNOWFLAKE_TEST_ACCOUNT", true)
	user := env("SNOWFLAKE_TEST_USER", true)
	password := env("SNOWFLAKE_TEST_PASSWORD", true)
	host := env("SNOWFLAKE_TEST_HOST", false)
	portStr := env("SNOWFLAKE_TEST_PORT", false)
	protocol := env("SNOWFLAKE_TEST_PROTOCOL", false)

	port := 443
	var err error
	if len(portStr) > 0 {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return "", nil, err
		}
	}

	cfg := &sf.Config{
		Account:  account,
		User:     user,
		Password: password,
		Host:     host,
		Port:     port,
		Protocol: protocol,
	}

	dsn, err := sf.DSN(cfg)
	return dsn, cfg, err
}

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	dsn, config, err := getDSN()
	if err != nil {
		log.Fatalf("Failed to create DSN from config: %v, error: %v", config, err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("Failed to connect, dsn: %v, error: %v", dsn, err)
	}

	defer db.Close()

	numberOfQueries := 2
	query := `
		WITH table1 AS (SELECT 'table 1, row 1', 11 UNION SELECT 'table 1, row 2', 12)
		SELECT * FROM table1;

		WITH table2 AS (SELECT 'table 2, row 1', 21 UNION SELECT 'table 2, row 2', 22)
		SELECT * FROM table2;
	`

	context, err := sf.WithMultiStatement(context.Background(), numberOfQueries)
	if err != nil {
		log.Fatalf("Error while creating multi statement context: %v", err)
	}

	result, err := db.QueryContext(context, query)
	if err != nil {
		log.Fatalf("Error while querying snowflake: %v", err)
	}

	defer result.Close()

	for result.NextResultSet() {
		for result.Next() {
			var str string
			var int int
			err := result.Scan(&str, &int)
			if err != nil {
				log.Fatalf("Error while scanning row: %v", err)
			}
			fmt.Println(str, " | ", int)
		}
	}
	if result.Err() != nil {
		log.Fatalf("Error while reading results: %v", err)
	}
}
