// This code is to profile a large result set query. It is basically similar to selectmany example code but
// leverages benchmark framework.
package largesetresult

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

	sf "github.com/snowflakedb/gosnowflake"
)

func TestLargeResultSet(t *testing.T) {
	runLargeResultSet()
}

func BenchmarkLargeResultSet(*testing.B) {
	runLargeResultSet()
}

func runLargeResultSet() {
	if !flag.Parsed() {
		flag.Parse()
	}

	// handler interrupt signal
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	defer close(c)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
	}()
	go func() {
		select {
		case <-c:
			cancel()
		case <-ctx.Done():
		}
	}()

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
	defer db.Close()
	if err != nil {
		log.Fatalf("failed to connect. err: %v", err)
	}

	query := `select * from
	  (select 0 a union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) A,
	  (select 0 b union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) B,
	  (select 0 c union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) C,
	  (select 0 d union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) E,
	  (select 0 e union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) F,
	  (select 0 f union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) G,
	  (select 0 f union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) H`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	defer rows.Close()
	var v1, v2, v3, v4, v5, v6, v7 int
	counter := 0
	for rows.Next() {
		if err = rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6, &v7); err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		}
		if counter%1000000 == 0 {
			debug.FreeOSMemory()
		}
		counter++
	}
}
