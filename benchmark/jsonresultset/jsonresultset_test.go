// This code is to profile a large json result set query. It is basically similar to selectmany example code but
// leverages benchmark framework.
package jsonresultset

import (
	"flag"
	"log"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"testing"

	"database/sql"

	"context"
	"os/signal"

	"runtime/debug"

	sf "github.com/snowflakedb/gosnowflake"
)

func TestJsonResultSet(t *testing.T) {
	runJSONResultSet()
}

func BenchmarkJsonResultSet(*testing.B) {
	runJSONResultSet()
}

func runJSONResultSet() {
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
	setCustomJSONDecoder()
	setMaxChunkDownloadWorkers()
	cfg, err := sf.GetConfigFromEnv([]*sf.ConfigParam{
		{Name: "Account", EnvName: "SNOWFLAKE_TEST_ACCOUNT", FailOnMissing: true},
		{Name: "User", EnvName: "SNOWFLAKE_TEST_USER", FailOnMissing: true},
		{Name: "Password", EnvName: "SNOWFLAKE_TEST_PASSWORD", FailOnMissing: true},
		{Name: "Role", EnvName: "SNOWFLAKE_TEST_ROLE", FailOnMissing: false},
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

func setMaxChunkDownloadWorkers() {
	maxChunkDownloadWorkersStr, err := sf.GetFromEnv("SNOWFLAKE_TEST_MAX_CHUNK_DOWNLOAD_WORKERS", false)
	if err != nil {
		log.Fatal(err)
	}
	if maxChunkDownloadWorkersStr != "" {
		maxChunkDownloadWorkers, err := strconv.Atoi(maxChunkDownloadWorkersStr)
		if err != nil {
			log.Fatalf("invalid value for SNOWFLAKE_TEST_MAX_CHUNK_DOWNLOAD_WORKERS: %v", maxChunkDownloadWorkers)
		}
		sf.MaxChunkDownloadWorkers = maxChunkDownloadWorkers
	}
}

func setCustomJSONDecoder() {
	customJSONDecoderEnabledStr, err := sf.GetFromEnv("SNOWFLAKE_TEST_CUSTOME_JSON_DECODER_ENABLED", true)
	if err != nil {
		log.Fatal(err)
	}
	sf.CustomJSONDecoderEnabled = strings.EqualFold("true", customJSONDecoderEnabledStr)
}
