// This code is to profile a large json result set query. It is basically similar to selectmany example code but
// leverages benchmark framework.
package jsonresultset

import (
	"flag"
	"log"
	_ "net/http/pprof"
	"os"
	"testing"

	"database/sql"

	"context"
	"os/signal"

	"runtime/debug"

	"strconv"

	sf "github.com/snowflakedb/gosnowflake"
)

func TestJsonResultSet(t *testing.T) {
	runJsonResultSet()
}

func BenchmarkJsonResultSet(*testing.B) {
	runJsonResultSet()
}

// getDSN constructs a DSN based on the test connection parameters
func getDSN() (dsn string, cfg *sf.Config, err error) {
	env := func(k string, failOnMissing bool) string {
		if value := os.Getenv(k); value != "" {
			return value
		}
		if failOnMissing {
			log.Fatalf("%v environment variable is not set.", k)
		}
		return ""
	}

	account := env("SNOWFLAKE_TEST_ACCOUNT", true)
	user := env("SNOWFLAKE_TEST_USER", true)
	password := env("SNOWFLAKE_TEST_PASSWORD", true)
	host := env("SNOWFLAKE_TEST_HOST", false)
	port := env("SNOWFLAKE_TEST_PORT", false)
	protocol := env("SNOWFLAKE_TEST_PROTOCOL", false)
	role := env("SNOWFLAKE_TEST_ROLE", false)

	portStr, _ := strconv.Atoi(port)
	cfg = &sf.Config{
		Account:  account,
		User:     user,
		Password: password,
		Host:     host,
		Role:     role,
		Port:     portStr,
		Protocol: protocol,
	}

	dsn, err = sf.DSN(cfg)
	return dsn, cfg, err
}

func runJsonResultSet() {
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

	dsn, cfg, err := getDSN()
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	db, err := sql.Open("snowflake", dsn)
	defer db.Close()
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}

	query := `SELECT V FROM SNOWFLAKE_SAMPLE_DATA.WEATHER.HOURLY_14_TOTAL LIMIT 100000`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	defer rows.Close()
	var v1 string
	counter := 0
	for rows.Next() {
		err := rows.Scan(&v1)
		if err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		}
		if counter%1000000 == 0 {
			debug.FreeOSMemory()
		}
		counter++
	}
}
