// Example: client session keep alive
// By default, the token expires in 4 hours if the connection is idle. CLIENT_SESSION_KEEP_ALIVE parameter will
// have a heartbeat in the background to keep the connection alive by making explicit heartbeats
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	sf "github.com/snowflakedb/gosnowflake"
)

func runQuery(db *sql.DB, query string) {
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

}

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}

	cfg, err := sf.GetConfigFromEnv([]sf.ConfigParam{
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
	runQuery(db, query)

	time.Sleep(90 * time.Minute)

	runQuery(db, query)

	time.Sleep(4 * time.Hour)

	runQuery(db, query)

	time.Sleep(8 * time.Hour)

	fmt.Printf("Congrats! You have successfully run %v with Snowflake DB!\n", query)
}
