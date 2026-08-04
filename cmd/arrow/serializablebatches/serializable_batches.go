// This example demonstrates distributed fetch of Arrow batches.
//
// A "producer" runs a query and obtains ArrowBatch handles, converts each to a
// SerializableArrowBatch, and JSON-encodes it. The encoded bytes could be shipped to
// other machines (e.g. Spark/Hadoop workers). Here we simulate the workers in-process:
// each worker decodes a descriptor and fetches its chunk using a plain *http.Client with
// NO Snowflake connection — the presigned chunk URL and SSE-C headers are all it needs.
package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"

	sf "github.com/snowflakedb/gosnowflake/v2"
	"github.com/snowflakedb/gosnowflake/v2/arrowbatches"
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
		{Name: "Warehouse", EnvName: "SNOWFLAKE_TEST_WAREHOUSE", FailOnMissing: false},
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

	// ---- Producer: run the query and serialize the batches ----
	descriptors := produce(db)
	fmt.Printf("producer serialized %d batches\n", len(descriptors))

	// ---- Close the Snowflake connection entirely ----
	// The workers below fetch chunks with no Snowflake session at all — only the serialized
	// descriptors and a plain HTTP client. Closing the DB here makes that independence
	// explicit: everything after this point runs without a live connection.
	if err := db.Close(); err != nil {
		log.Fatalf("failed to close db: %v", err)
	}

	// ---- Ship the descriptors (here: just the encoded bytes) ----
	// In a real deployment these bytes would be sent to remote workers. NOTE: a descriptor
	// embeds the presigned URL and SSE-C decryption key, so treat it as a credential and
	// send it only over secure channels. Presigned URLs also expire after a few hours.

	// ---- Workers: reconstruct and fetch, with no Snowflake connection ----
	totalRows := consume(descriptors)
	fmt.Printf("workers fetched %d rows in total\n", totalRows)
}

// produce runs a query in arrow-batches mode and returns each batch as JSON-encoded
// SerializableArrowBatch bytes.
func produce(db *sql.DB) [][]byte {
	ctx := arrowbatches.WithArrowBatches(context.Background())
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatalf("cannot create a connection from the pool. err: %v", err)
	}
	defer conn.Close()

	query := "SELECT SEQ8() AS n, RANDSTR(200, RANDOM()) AS s FROM TABLE(GENERATOR(ROWCOUNT=>100000))"
	var rows driver.Rows
	if err = conn.Raw(func(x any) error {
		rows, err = x.(driver.QueryerContext).QueryContext(ctx, query, nil)
		return err
	}); err != nil {
		log.Fatalf("unable to run the query. err: %v", err)
	}
	defer rows.Close()

	batches, err := arrowbatches.GetArrowBatches(rows.(sf.SnowflakeRows))
	if err != nil {
		log.Fatalf("unable to get arrow batches. err: %v", err)
	}

	descriptors := make([][]byte, len(batches))
	for i, b := range batches {
		s, err := b.ToSerializable()
		if err != nil {
			log.Fatalf("unable to serialize batch %d. err: %v", i, err)
		}
		data, err := json.Marshal(s)
		if err != nil {
			log.Fatalf("unable to marshal batch %d. err: %v", i, err)
		}
		descriptors[i] = data
	}
	return descriptors
}

// consume simulates distributed workers: each decodes a descriptor and fetches its chunk
// using a fresh HTTP client, with no Snowflake session.
func consume(descriptors [][]byte) int64 {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)

	var total int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i, data := range descriptors {
		wg.Add(1)
		go func(workerID int, data []byte) {
			defer wg.Done()

			var s arrowbatches.SerializableArrowBatch
			if err := json.Unmarshal(data, &s); err != nil {
				log.Fatalf("worker %d: unmarshal failed: %v", workerID, err)
			}
			batch, err := s.ToArrowBatch(
				arrowbatches.WithHTTPClient(&http.Client{}),
				arrowbatches.WithAllocator(pool),
			)
			if err != nil {
				log.Fatalf("worker %d: ToArrowBatch failed: %v", workerID, err)
			}
			records, err := batch.Fetch()
			if err != nil {
				log.Fatalf("worker %d: Fetch failed: %v", workerID, err)
			}
			var rows int64
			for _, rec := range *records {
				rows += rec.NumRows()
				rec.Release()
			}
			mu.Lock()
			total += rows
			mu.Unlock()
		}(i, data)
	}
	wg.Wait()

	if pool.CurrentAlloc() != 0 {
		fmt.Printf("Memory leak detected: %d bytes still allocated\n", pool.CurrentAlloc())
	} else {
		fmt.Println("No memory leaks detected.")
	}
	return total
}
