// This sample code demonstrates how to fetch many rows and allow cancel the query by Ctrl+C.
// The difference to other selectmany sample code is this uses the method Query instead of QueryContext such that the default context is used.

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"

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
	defer db.Close()
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	query := "SELECT seq8(), randstr(5, random()) from table(generator(rowcount=>10000000))"
	fmt.Printf("Executing a query. It may take long. You may stop by Ctrl+C.\n")
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	defer rows.Close()
	var v int
	var s string
	fmt.Printf("Fetching the results. It may take long. You may stop by Ctrl+C.\n")
	for rows.Next() {
		err := rows.Scan(&v, &s)
		if err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		}
		if v%10000 == 0 {
			fmt.Printf("idx: %v, data: %v\n", v, s)
		}
	}
	if rows.Err() != nil {
		fmt.Printf("ERROR: %v\n", rows.Err())
		return
	}
	fmt.Printf("Congrats! You have successfully run %v with Snowflake DB!\n", query)
}
