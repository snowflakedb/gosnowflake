// Example: No connection pool
//
// Setting the pool size to 1, a single session is used at all times. This guarantees USE <object type> <object name>
// changes the current working object and subsequent commands can have the access.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sync"

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
		log.Fatalf("failed to connect. err: %v", err)
	}
	// single session
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	defer db.Close()

	var wg sync.WaitGroup
	n := 10
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			query := "select current_session()"
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
				fmt.Printf("Session: %v\n", v)
			}
			if rows.Err() != nil {
				fmt.Printf("ERROR: %v\n", rows.Err())
				return
			}
		}()
	}
	fmt.Println("Waiting to finish...")
	wg.Wait()
	fmt.Printf("Congrats! You have successfully!\n")
}
