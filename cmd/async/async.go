package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	sf "github.com/snowflakedb/gosnowflake"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

func getDSN() (string, *sf.Config, error) {
	env := func(k string, failOnMissing bool) string {
		if value := os.Getenv(k); value != "" {
			return value
		}
		if failOnMissing {
			log.Fatalf("%v environment variable is not set.", k)
		}
		return ""
	}

	account := env("SNOWFLAKE_TEST_ACCOUNT", true)
	user := env("SNOWFLAKE_TEST_USER", true)
	password := env("SNOWFLAKE_TEST_PASSWORD", true)
	host := env("SNOWFLAKE_TEST_HOST", false)
	portStr := env("SNOWFLAKE_TEST_PORT", false)
	protocol := env("SNOWFLAKE_TEST_PROTOCOL", false)

	port := 443 // snowflake default port
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

	dsn, cfg, err := getDSN()
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()

	var rows driver.Rows
	fmt.Println("Lets simulate long running query")
	rows, err = runAsyncQuery(db, "CALL SYSTEM$WAIT(10, 'SECONDS')")
	fmt.Println("The query is running asynchronously")
	printResult(rows)

	fmt.Println("Lets simulate long running query using the second way")
	rows2, err := runAsyncQuery2(db, "CALL SYSTEM$WAIT(10, 'SECONDS')")
	fmt.Println("The query is running asynchronously")
	printResult2(rows2)
}

func runAsyncQuery(db *sql.DB, query string) (driver.Rows, error) {
	// Enable asynchronous mode
	ctx := sf.WithAsyncMode(context.Background())

	// Establish a connection
	conn, _ := db.Conn(ctx)
	var rows driver.Rows

	// Unwrap connection
	err := conn.Raw(func(x interface{}) error {
		var err error
		// Execute asynchronous query
		rows, err = x.(driver.QueryerContext).QueryContext(ctx, query, nil)

		return err
	})

	return rows, err
}

func runAsyncQuery2(db *sql.DB, query string) (*sql.Rows, error) {
	// Enable asynchronous mode
	ctx := sf.WithAsyncMode(context.Background())

	// Execute asynchronous query
	rows, err := db.QueryContext(ctx, query)

	return rows, err
}

func printResult(rows driver.Rows) {
	fmt.Println(strings.Join(rows.Columns(), ", "))

	dest := make([]driver.Value, 1)
	for rows.Next(dest) != io.EOF {
		for val := range dest {
			fmt.Printf("%v\n", dest[val])
		}
	}
}

func printResult2(rows *sql.Rows) {
	cols, err := rows.Columns()
	if err != nil {
		log.Fatalf("failed to get columns. err: %v", err)
	}
	fmt.Println(strings.Join(cols, ", "))

	var val string
	for rows.Next() {
		err := rows.Scan(&val)
		if err != nil {
			log.Fatalf("failed to scan rows. err: %v", err)
		}
		fmt.Printf("%v\n", val)
	}
}
