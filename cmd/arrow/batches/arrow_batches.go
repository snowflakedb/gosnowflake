package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"log"
	"sync"

	sf "github.com/snowflakedb/gosnowflake"
)

type sampleRecord struct {
	batchID  int
	workerID int
	number   int32
	string   string
}

func (s sampleRecord) String() string {
	return fmt.Sprintf("batchID: %v, workerID: %v, number: %v, string: %v", s.batchID, s.workerID, s.number, s.string)
}

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

	ctx := sf.WithArrowAllocator(sf.WithArrowBatches(context.Background()), memory.DefaultAllocator)
	query := "SELECT SEQ4(), 'example ' || (SEQ4() * 2) FROM TABLE(GENERATOR(ROWCOUNT=>30000))"

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()

	conn, _ := db.Conn(ctx)
	defer conn.Close()

	var rows driver.Rows
	err = conn.Raw(func(x interface{}) error {
		rows, err = x.(driver.QueryerContext).QueryContext(ctx, query, nil)
		return err
	})
	if err != nil {
		log.Fatalf("unable to run the query. err: %v", err)
	}
	defer rows.Close()

	batches, err := rows.(sf.SnowflakeRows).GetArrowBatches()
	batchIds := make(chan int, 1)
	maxWorkers := len(batches)
	sampleRecordsPerBatch := make([][]sampleRecord, len(batches))

	var waitGroup sync.WaitGroup
	for workerID := 0; workerID < maxWorkers; workerID++ {
		waitGroup.Add(1)
		go func(waitGroup *sync.WaitGroup, batchIDs chan int, workerId int) {
			defer waitGroup.Done()

			for batchID := range batchIDs {
				records, err := batches[batchID].Fetch()
				if err != nil {
					log.Fatalf("Error while fetching batch %v: %v", batchID, err)
				}
				sampleRecordsPerBatch[batchID] = make([]sampleRecord, batches[batchID].GetRowCount())
				totalRowID := 0
				convertFromColumnsToRows(records, sampleRecordsPerBatch, batchID, workerId, totalRowID)
			}
		}(&waitGroup, batchIds, workerID)
	}

	for batchID := 0; batchID < len(batches); batchID++ {
		batchIds <- batchID
	}
	close(batchIds)
	waitGroup.Wait()

	for _, batchSampleRecords := range sampleRecordsPerBatch {
		for _, sampleRecord := range batchSampleRecords {
			fmt.Println(sampleRecord)
		}
	}
	for batchID, batch := range batches {
		fmt.Printf("BatchId: %v, number of records: %v\n", batchID, batch.GetRowCount())
	}
}

func convertFromColumnsToRows(records *[]arrow.Record, sampleRecordsPerBatch [][]sampleRecord, batchID int,
	workerID int, totalRowID int) {
	for _, record := range *records {
		for rowID, intColumn := range record.Column(0).(*array.Int32).Int32Values() {
			sampleRecord := sampleRecord{
				batchID:  batchID,
				workerID: workerID,
				number:   intColumn,
				string:   record.Column(1).(*array.String).Value(rowID),
			}
			sampleRecordsPerBatch[batchID][totalRowID] = sampleRecord
			totalRowID++
		}
	}
}
