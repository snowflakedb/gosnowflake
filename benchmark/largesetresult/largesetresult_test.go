// This code is to profile a large result set query. It is basically similar to selectmany example code but
// leverages benchmark framework.
package largesetresult

import (
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"testing"

	"database/sql"

	"context"
	_ "github.com/snowflakedb/gosnowflake"
	"os/signal"
)

func TestLargeResultSet(t *testing.T) {
	runLargeResultSet()
}

func BenchmarkLargeResultSet(*testing.B) {
	runLargeResultSet()
}

func runLargeResultSet() {
	if !flag.Parsed() {
		// enable glog for Go Snowflake Driver
		flag.Parse()
	}

	// handler interrupt signal
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
	}()
	go func() {
		<-c
		log.Println("Caught signal, canceling...")
		cancel()
	}()

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

	query := "SELECT seq8(), randstr(100, random()) FROM table(generator(rowcount=>100000))"
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	defer rows.Close()
	var v int
	var s string
	for rows.Next() {
		err := rows.Scan(&v, &s)
		if err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		}
	}
}
