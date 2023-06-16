package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"

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
	dsn, err := sf.DSN(cfg)
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()

	fmt.Println("Lets simulate long running query by passing execution logic as a function")
	driverRows := runAsyncDriverQuery(db, "CALL SYSTEM$WAIT(10, 'SECONDS')")
	fmt.Println("The query is running asynchronously - you can continue your workflow after starting the query")
	printDriverRowsResult(driverRows)

	fmt.Println("Lets simulate long running query using the standard sql package")
	sqlRows := runAsyncSQLQuery(db, "CALL SYSTEM$WAIT(10, 'SECONDS')")
	fmt.Println("The query is running asynchronously - you can continue your workflow after starting the query")
	printSQLRowsResult(sqlRows)
}

func runAsyncDriverQuery(db *sql.DB, query string) driver.Rows {
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

	if err != nil {
		log.Fatalf("unable to run the query. err: %v", err)
	}

	return rows
}

func runAsyncSQLQuery(db *sql.DB, query string) *sql.Rows {
	// Enable asynchronous mode
	ctx := sf.WithAsyncMode(context.Background())

	// Execute asynchronous query
	rows, err := db.QueryContext(ctx, query)

	if err != nil {
		log.Fatalf("unable to run the query. err: %v", err)
	}

	return rows
}

func printDriverRowsResult(rows driver.Rows) {
	fmt.Println(strings.Join(rows.Columns(), ", "))

	dest := make([]driver.Value, 1)
	for rows.Next(dest) != io.EOF {
		for val := range dest {
			fmt.Printf("%v\n", dest[val])
		}
	}
}

func printSQLRowsResult(rows *sql.Rows) {
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
