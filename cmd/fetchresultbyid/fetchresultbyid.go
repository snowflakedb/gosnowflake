package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
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

	dsn, err := sf.DSN(cfg)
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	query := "SELECT 1"
	ctx := context.Background()

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatalf("failed to get Conn. err: %v", err)
	}
	defer conn.Close()

	var rows1 driver.Rows
	var queryID string

	// Get the query ID using raw connection
	err = conn.Raw(func(x any) error {
		rows1, err = x.(driver.QueryerContext).QueryContext(ctx, query, nil)
		if err != nil {
			return err
		}

		queryID = rows1.(sf.SnowflakeRows).GetQueryID()
		return nil
	})
	if err != nil {
		log.Fatalf("unable to run the query. err: %v", err)
	}

	// Update the Context object to specify the query ID
	ctx = sf.WithFetchResultByID(ctx, queryID)

	// Execute an empty string query
	rows2, err := db.QueryContext(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	defer rows2.Close()

	// Retrieve the results as usual
	var v int
	for rows2.Next() {
		err = rows2.Scan(&v)
		if err != nil {
			log.Fatal(err)
		}
		if v != 1 {
			log.Fatal(err)
		}
	}
	if rows2.Err() != nil {
		fmt.Printf("ERROR: %v\n", rows2.Err())
	}

	fmt.Printf("Congrats! You have successfully run %v with Snowflake DB!\n", query)
}
