package largesetresult

import (
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"testing"
	"time"

	"database/sql"
	"net/http"

	_ "github.com/snowflakedb/gosnowflake"
)

func BenchmarkLargeResultSet(*testing.B) {
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

	query := "SELECT seq8(), randstr(100, random()) FROM table(generator(rowcount=>100000))"
	rows, err := db.Query(query)
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
		//if v%100 == 0 {
		//	fmt.Printf("%v: %v\n", v, s)
		//}
	}
	// launch pprof HTTP server so that the profile can be retrieved.
	// Heap: go tool pprof http://localhost:6060/debug/pprof/heap
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	time.Sleep(1 * time.Second)
}
