package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
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

	log.Println("Lets simulate running synchronous query and fetching the result by the query ID using the WithFetchResultByID context")
	sqlRows := fetchResultByIDSync(db, "SELECT 1")
	printSQLRowsResult(sqlRows)

	log.Println("Lets simulate running long query asynchronously and fetching result by query ID using a channel provided in the WithQueryIDChan context")
	sqlRows = fetchResultByIDAsync(db, "CALL SYSTEM$WAIT(10, 'SECONDS')")
	printSQLRowsResult(sqlRows)
}

func fetchResultByIDSync(db *sql.DB, query string) *sql.Rows {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatalf("failed to get Conn. err: %v", err)
	}
	defer conn.Close()

	var rows1 driver.Rows
	var queryID string

	// Get the query ID using raw connection
	err = conn.Raw(func(x any) error {
		log.Printf("Executing query: %v\n", query)
		rows1, err = x.(driver.QueryerContext).QueryContext(ctx, query, nil)
		if err != nil {
			return err
		}

		queryID = rows1.(sf.SnowflakeRows).GetQueryID()
		log.Printf("Query ID retrieved from GetQueryID(): %v\n", queryID)
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

	return rows2
}

func fetchResultByIDAsync(db *sql.DB, query string) *sql.Rows {
	// Make a channel to receive the query ID
	queryIDChan := make(chan string, 1)

	// Enable asynchronous mode
	ctx := sf.WithAsyncMode(context.Background())

	// Pass the channel to receive the query ID
	ctx = sf.WithQueryIDChan(ctx, queryIDChan)

	// Run a long running query asynchronously and without retrieving the result
	log.Printf("Executing query: %v\n", query)
	go func() {
		_, err := db.ExecContext(ctx, query)
		if err != nil {
			log.Fatal(err)
		}
	}()

	// Get the query ID without waiting for the query to finish
	queryID := <-queryIDChan
	log.Printf("Query ID retrieved from the channel: %v\n", queryID)

	// Update the Context object to specify the query ID
	ctx = sf.WithFetchResultByID(ctx, queryID)

	// Execute an empty string query
	rows, err := db.QueryContext(ctx, "")
	if err != nil {
		log.Fatal(err)
	}

	return rows
}

func printSQLRowsResult(rows *sql.Rows) {
	log.Print("Printing the results: \n")

	cols, err := rows.Columns()
	if err != nil {
		log.Fatalf("failed to get columns. err: %v", err)
	}
	log.Println(strings.Join(cols, ", "))

	var val string
	for rows.Next() {
		err := rows.Scan(&val)
		if err != nil {
			log.Fatalf("failed to scan rows. err: %v", err)
		}
		log.Printf("%v\n", val)
	}
}
