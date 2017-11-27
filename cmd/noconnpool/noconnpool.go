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
	"os"
	"sync"

	_ "github.com/snowflakedb/gosnowflake"
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

	dsn := fmt.Sprintf("%v:%v@%v", user, password, account)
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
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
