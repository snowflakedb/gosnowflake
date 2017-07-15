package largesetresult

import (
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"testing"

	"database/sql"

	_ "github.com/snowflakedb/gosnowflake"
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
}
