package gosnowflake

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	ia "github.com/snowflakedb/gosnowflake/v2/internal/arrow"
)

func TestBadChunkData(t *testing.T) {
	testDecodeErr(t, "")
	testDecodeErr(t, "null")
	testDecodeErr(t, "42")
	testDecodeErr(t, "\"null\"")
	testDecodeErr(t, "{}")

	testDecodeErr(t, "[[]")
	testDecodeErr(t, "[null]")
	testDecodeErr(t, `[[hello world]]`)

	testDecodeErr(t, `[[""hello world""]]`)
	testDecodeErr(t, `[["\"hello world""]]`)
	testDecodeErr(t, `[[""hello world\""]]`)
	testDecodeErr(t, `[["hello world`)
	testDecodeErr(t, `[["hello world"`)
	testDecodeErr(t, `[["hello world"]`)

	testDecodeErr(t, `[["\uQQQQ"]]`)

	for b := byte(0); b < ' '; b++ {
		testDecodeErr(t, string([]byte{
			'[', '[', '"', b, '"', ']', ']',
		}))
	}
}

func TestValidChunkData(t *testing.T) {
	testDecodeOk(t, "[]")
	testDecodeOk(t, "[  ]")
	testDecodeOk(t, "[[]]")
	testDecodeOk(t, "[ [  ]   ]")
	testDecodeOk(t, "[[],[],[],[]]")
	testDecodeOk(t, "[[] , []  , [], []  ]")

	testDecodeOk(t, "[[null]]")
	testDecodeOk(t, "[[\n\t\r null]]")
	testDecodeOk(t, "[[null,null]]")
	testDecodeOk(t, "[[ null , null ]]")
	testDecodeOk(t, "[[null],[null],[null]]")
	testDecodeOk(t, "[[null],[ null  ] ,  [null]]")

	testDecodeOk(t, `[[""]]`)
	testDecodeOk(t, `[["false"]]`)
	testDecodeOk(t, `[["true"]]`)
	testDecodeOk(t, `[["42"]]`)

	testDecodeOk(t, `[[""]]`)
	testDecodeOk(t, `[["hello"]]`)
	testDecodeOk(t, `[["hello world"]]`)

	testDecodeOk(t, `[["/ ' \\ \b \t \n \f \r \""]]`)
	testDecodeOk(t, `[["❄"]]`)
	testDecodeOk(t, `[["\u2744"]]`)
	testDecodeOk(t, `[["\uFfFc"]]`)       // consume replacement chars
	testDecodeOk(t, `[["\ufffd"]]`)       // consume replacement chars
	testDecodeOk(t, `[["\u0000"]]`)       // yes, this is valid
	testDecodeOk(t, `[["\uD834\uDD1E"]]`) // surrogate pair
	testDecodeOk(t, `[["\uD834\u0000"]]`) // corrupt surrogate pair

	testDecodeOk(t, `[["$"]]`)      // "$"
	testDecodeOk(t, `[["\u0024"]]`) // "$"

	testDecodeOk(t, `[["\uC2A2"]]`) // "¢"
	testDecodeOk(t, `[["¢"]]`)      // "¢"

	testDecodeOk(t, `[["\u00E2\u82AC"]]`) // "€"
	testDecodeOk(t, `[["€"]]`)            // "€"

	testDecodeOk(t, `[["\uF090\u8D88"]]`) // "𐍈"
	testDecodeOk(t, `[["𐍈"]]`)            // "𐍈"
}

func TestSmallBufferChunkData(t *testing.T) {
	r := strings.NewReader(`[
	  [null,"hello world"],
	  ["foo bar", null],
	  [null, null] ,
	  ["foo bar",   "hello world" ]
	]`)

	lcd := largeChunkDecoder{
		r, 0, 0,
		0, 0,
		make([]byte, 1),
		bytes.NewBuffer(make([]byte, defaultStringBufferSize)),
		nil,
	}

	if _, err := lcd.decode(); err != nil {
		t.Fatalf("failed with small buffer: %s", err)
	}
}

func TestEnsureBytes(t *testing.T) {
	// the content here doesn't matter
	r := strings.NewReader("0123456789")

	lcd := largeChunkDecoder{
		r, 0, 0,
		3, 8189,
		make([]byte, 8192),
		bytes.NewBuffer(make([]byte, defaultStringBufferSize)),
		nil,
	}

	lcd.ensureBytes(4)

	// we expect the new remainder to be 3 + 10 (length of r)
	if lcd.rem != 13 {
		t.Fatalf("buffer was not refilled correctly")
	}
}

func testDecodeOk(t *testing.T, s string) {
	var rows [][]*string
	if err := json.Unmarshal([]byte(s), &rows); err != nil {
		t.Fatalf("test case is not valid json / [][]*string: %s", s)
	}

	// NOTE we parse and stringify the expected result to
	// remove superficial differences, like whitespace
	expect, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("unreachable: %s", err)
	}

	rows, err = decodeLargeChunk(strings.NewReader(s), 0, 0)
	if err != nil {
		t.Fatalf("expected decode to succeed: %s", err)
	}

	actual, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("json marshal failed: %s", err)
	}
	if string(actual) != string(expect) {
		t.Fatalf(`
		result did not match expected result
		  expect=%s
		   bytes=(%v)

		  acutal=%s
		   bytes=(%v)`,
			string(expect), expect,
			string(actual), actual,
		)
	}
}

func testDecodeErr(t *testing.T, s string) {
	if _, err := decodeLargeChunk(strings.NewReader(s), 0, 0); err == nil {
		t.Fatalf("expected decode to fail for input: %s", s)
	}
}

func TestEnableArrowBatches(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		ctx := ia.EnableArrowBatches(sct.sc.ctx)
		numrows := 3000 // approximately 6 ArrowBatch objects

		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx = WithArrowAllocator(ctx, pool)

		query := fmt.Sprintf(selectRandomGenerator, numrows)
		rows := sct.mustQueryContext(ctx, query, []driver.NamedValue{})
		defer rows.Close()

		// getting result batches via raw bridge
		info, err := rows.(*snowflakeRows).GetArrowBatches()
		if err != nil {
			t.Error(err)
		}
		batches := info.Batches
		numBatches := len(batches)
		maxWorkers := 10 // enough for 3000 rows
		type count struct {
			m       sync.Mutex
			recVal  int
			metaVal int
		}
		cnt := count{recVal: 0}
		var wg sync.WaitGroup
		chunks := make(chan int, numBatches)

		for w := 1; w <= maxWorkers; w++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup, chunks <-chan int) {
				defer wg.Done()

				for i := range chunks {
					batch := batches[i]
					var recs *[]arrow.Record
					if batch.Records != nil {
						recs = batch.Records
					} else if batch.Download != nil {
						var downloadErr error
						recs, _, downloadErr = batch.Download(context.Background())
						if downloadErr != nil {
							t.Error(downloadErr)
						}
					}
					if recs != nil {
						for _, r := range *recs {
							cnt.m.Lock()
							cnt.recVal += int(r.NumRows())
							cnt.m.Unlock()
							r.Release()
						}
					}
					cnt.m.Lock()
					cnt.metaVal += batch.RowCount
					cnt.m.Unlock()
				}
			}(&wg, chunks)
		}
		for j := 0; j < numBatches; j++ {
			chunks <- j
		}
		close(chunks)

		wg.Wait()
		if cnt.recVal != numrows {
			t.Errorf("number of rows from records didn't match. expected: %v, got: %v", numrows, cnt.recVal)
		}
		if cnt.metaVal != numrows {
			t.Errorf("number of rows from arrow batch metadata didn't match. expected: %v, got: %v", numrows, cnt.metaVal)
		}
	})
}

func TestWithArrowBatchesAsync(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		ctx := WithAsyncMode(sct.sc.ctx)
		ctx = ia.EnableArrowBatches(ctx)
		numrows := 50000

		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx = WithArrowAllocator(ctx, pool)

		query := fmt.Sprintf(selectRandomGenerator, numrows)
		rows := sct.mustQueryContext(ctx, query, []driver.NamedValue{})
		defer rows.Close()

		info, err := rows.(*snowflakeRows).GetArrowBatches()
		if err != nil {
			t.Error(err)
		}
		batches := info.Batches
		numBatches := len(batches)
		maxWorkers := 10
		type count struct {
			m       sync.Mutex
			recVal  int
			metaVal int
		}
		cnt := count{recVal: 0}
		var wg sync.WaitGroup
		chunks := make(chan int, numBatches)

		for w := 1; w <= maxWorkers; w++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup, chunks <-chan int) {
				defer wg.Done()

				for i := range chunks {
					batch := batches[i]
					var recs *[]arrow.Record
					if batch.Records != nil {
						recs = batch.Records
					} else if batch.Download != nil {
						var downloadErr error
						recs, _, downloadErr = batch.Download(context.Background())
						if downloadErr != nil {
							t.Error(downloadErr)
						}
					}
					if recs != nil {
						for _, r := range *recs {
							cnt.m.Lock()
							cnt.recVal += int(r.NumRows())
							cnt.m.Unlock()
							r.Release()
						}
					}
					cnt.m.Lock()
					cnt.metaVal += batch.RowCount
					cnt.m.Unlock()
				}
			}(&wg, chunks)
		}
		for j := 0; j < numBatches; j++ {
			chunks <- j
		}
		close(chunks)

		wg.Wait()
		if cnt.recVal != numrows {
			t.Errorf("number of rows from records didn't match. expected: %v, got: %v", numrows, cnt.recVal)
		}
		if cnt.metaVal != numrows {
			t.Errorf("number of rows from arrow batch metadata didn't match. expected: %v, got: %v", numrows, cnt.metaVal)
		}
	})
}

func TestWithArrowBatchesButReturningJSON(t *testing.T) {
	testWithArrowBatchesButReturningJSON(t, false)
}

func TestWithArrowBatchesButReturningJSONAsync(t *testing.T) {
	testWithArrowBatchesButReturningJSON(t, true)
}

func testWithArrowBatchesButReturningJSON(t *testing.T, async bool) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		requestID := NewUUID()
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx := WithArrowAllocator(context.Background(), pool)
		ctx = ia.EnableArrowBatches(ctx)
		ctx = WithRequestID(ctx, requestID)
		if async {
			ctx = WithAsyncMode(ctx)
		}

		sct.mustExec(forceJSON, nil)
		rows := sct.mustQueryContext(ctx, "SELECT 'hello'", nil)
		defer rows.Close()
		_, err := rows.(ia.BatchDataProvider).GetArrowBatches()
		assertNotNilF(t, err)
		var se *SnowflakeError
		assertTrueE(t, errors.As(err, &se))
		assertEqualE(t, se.Message, errMsgNonArrowResponseInArrowBatches)
		assertEqualE(t, se.Number, ErrNonArrowResponseInArrowBatches)

		v := make([]driver.Value, 1)
		assertNilE(t, rows.Next(v))
		assertEqualE(t, v[0], "hello")
	})
}

func TestWithArrowBatchesMultistatement(t *testing.T) {
	testWithArrowBatchesMultistatement(t, false)
}

func TestWithArrowBatchesMultistatementAsync(t *testing.T) {
	testWithArrowBatchesMultistatement(t, true)
}

func testWithArrowBatchesMultistatement(t *testing.T, async bool) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		sct.mustExec("ALTER SESSION SET ENABLE_FIX_1758055_ADD_ARROW_SUPPORT_FOR_MULTI_STMTS = true", nil)
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx := WithMultiStatement(ia.EnableArrowBatches(WithArrowAllocator(context.Background(), pool)), 2)
		if async {
			ctx = WithAsyncMode(ctx)
		}
		driverRows := sct.mustQueryContext(ctx, "SELECT 'abc' UNION SELECT 'def' ORDER BY 1; SELECT 'ghi' UNION SELECT 'jkl' ORDER BY 1", nil)
		defer driverRows.Close()
		sfRows := driverRows.(SnowflakeRows)
		expectedResults := [][]string{{"abc", "def"}, {"ghi", "jkl"}}
		resultSetIdx := 0
		for hasNextResultSet := true; hasNextResultSet; hasNextResultSet = sfRows.NextResultSet() != io.EOF {
			info, err := driverRows.(ia.BatchDataProvider).GetArrowBatches()
			assertNilF(t, err)
			assertEqualF(t, len(info.Batches), 1)
			batch := info.Batches[0]
			assertNotNilF(t, batch.Records)
			records := *batch.Records
			assertEqualF(t, len(records), 1)
			record := records[0]
			defer record.Release()
			assertEqualF(t, record.Column(0).(*array.String).Value(0), expectedResults[resultSetIdx][0])
			assertEqualF(t, record.Column(0).(*array.String).Value(1), expectedResults[resultSetIdx][1])
			resultSetIdx++
		}
		assertEqualF(t, resultSetIdx, len(expectedResults))
		err := sfRows.NextResultSet()
		assertErrIsE(t, err, io.EOF)
	})
}

func TestWithArrowBatchesMultistatementWithJSONResponse(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		sct.mustExec(forceJSON, nil)
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx := WithMultiStatement(ia.EnableArrowBatches(WithArrowAllocator(context.Background(), pool)), 2)
		driverRows := sct.mustQueryContext(ctx, "SELECT 'abc' UNION SELECT 'def' ORDER BY 1; SELECT 'ghi' UNION SELECT 'jkl' ORDER BY 1", nil)
		defer driverRows.Close()
		sfRows := driverRows.(SnowflakeRows)
		resultSetIdx := 0
		for hasNextResultSet := true; hasNextResultSet; hasNextResultSet = sfRows.NextResultSet() != io.EOF {
			_, err := driverRows.(ia.BatchDataProvider).GetArrowBatches()
			assertNotNilF(t, err)
			var se *SnowflakeError
			assertTrueF(t, errors.As(err, &se))
			assertEqualE(t, se.Number, ErrNonArrowResponseInArrowBatches)
			assertEqualE(t, se.Message, errMsgNonArrowResponseInArrowBatches)
			resultSetIdx++
		}
		assertEqualF(t, resultSetIdx, 2)
		err := sfRows.NextResultSet()
		assertErrIsE(t, err, io.EOF)
	})
}

func TestWithArrowBatchesMultistatementWithLargeResultSet(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		sct.mustExec("ALTER SESSION SET ENABLE_FIX_1758055_ADD_ARROW_SUPPORT_FOR_MULTI_STMTS = true", nil)
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer pool.AssertSize(t, 0)
		ctx := WithMultiStatement(ia.EnableArrowBatches(WithArrowAllocator(context.Background(), pool)), 2)
		driverRows := sct.mustQueryContext(ctx, "SELECT 'abc' FROM TABLE(GENERATOR(ROWCOUNT => 1000000)); SELECT 'abc' FROM TABLE(GENERATOR(ROWCOUNT => 1000000))", nil)
		defer driverRows.Close()
		sfRows := driverRows.(SnowflakeRows)
		rowCount := 0
		for hasNextResultSet := true; hasNextResultSet; hasNextResultSet = sfRows.NextResultSet() != io.EOF {
			info, err := driverRows.(ia.BatchDataProvider).GetArrowBatches()
			assertNilF(t, err)
			assertTrueF(t, len(info.Batches) > 1)
			for _, batch := range info.Batches {
				var recs *[]arrow.Record
				if batch.Records != nil {
					recs = batch.Records
				} else if batch.Download != nil {
					recs, _, err = batch.Download(context.Background())
					assertNilF(t, err)
				}
				if recs != nil {
					for _, record := range *recs {
						defer record.Release()
						for i := 0; i < int(record.NumRows()); i++ {
							assertEqualF(t, record.Column(0).(*array.String).Value(i), "abc")
							rowCount++
						}
					}
				}
			}
		}
		err := sfRows.NextResultSet()
		assertErrIsE(t, err, io.EOF)
	})
}

func TestQueryArrowStream(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		numrows := 50000

		query := fmt.Sprintf(selectRandomGenerator, numrows)
		loader, err := sct.sc.QueryArrowStream(sct.sc.ctx, query)
		assertNilF(t, err)

		if loader.TotalRows() != int64(numrows) {
			t.Errorf("total numrows did not match expected, wanted %v, got %v", numrows, loader.TotalRows())
		}

		batches, err := loader.GetBatches()
		assertNilF(t, err)
		assertTrueF(t, len(batches) > 0, "should have at least one batch")
		assertTrueF(t, len(loader.RowTypes()) > 0, "should have row types")
	})
}

func TestQueryArrowStreamDescribeOnly(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		numrows := 50000

		query := fmt.Sprintf(selectRandomGenerator, numrows)
		loader, err := sct.sc.QueryArrowStream(WithDescribeOnly(sct.sc.ctx), query)
		assertNilF(t, err, "failed to run query")

		if loader.TotalRows() != 0 {
			t.Errorf("total numrows did not match expected, wanted 0, got %v", loader.TotalRows())
		}

		if len(loader.RowTypes()) != 2 {
			t.Errorf("rowTypes length did not match expected, wanted 2, got %v", len(loader.RowTypes()))
		}
	})
}

func TestRetainChunkWOHighPrecision(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		var rows driver.Rows
		var err error

		err = dbt.conn.Raw(func(connection interface{}) error {
			rows, err = connection.(driver.QueryerContext).QueryContext(ia.EnableArrowBatches(context.Background()), "select 0", nil)
			return err
		})
		assertNilF(t, err, "error running select 0 query")

		info, err := rows.(ia.BatchDataProvider).GetArrowBatches()
		assertNilF(t, err, "error getting arrow batch data")
		assertEqualF(t, len(info.Batches), 1, "should have one batch")

		records := info.Batches[0].Records
		assertNotNilF(t, records, "records should not be nil")

		numRecords := len(*records)
		assertEqualF(t, numRecords, 1, "should have exactly one record")

		record := (*records)[0]
		assertEqualF(t, len(record.Columns()), 1, "should have exactly one column")

		column := record.Column(0).(*array.Int8)
		row := column.Len()
		assertEqualF(t, row, 1, "should have exactly one row")

		int8Val := column.Value(0)
		assertEqualF(t, int8Val, int8(0), "value of cell should be 0")
	})
}

func TestQueryArrowStreamMultiStatement(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		sct.mustExec("ALTER SESSION SET ENABLE_FIX_1758055_ADD_ARROW_SUPPORT_FOR_MULTI_STMTS = true", nil)
		ctx := WithMultiStatement(ia.EnableArrowBatches(sct.sc.ctx), 2)
		loader, err := sct.sc.QueryArrowStream(ctx, "SELECT 'abc'; SELECT 'abc' UNION SELECT 'def' ORDER BY 1")
		assertNilF(t, err)
		assertTrueF(t, len(loader.RowTypes()) > 0, "should have row types")
		assertTrueF(t, loader.TotalRows() > 0, "should have total rows")
	})
}

func TestQueryArrowStreamMultiStatementForJSONData(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		ctx := WithMultiStatement(ia.EnableArrowBatches(sct.sc.ctx), 2)
		loader, err := sct.sc.QueryArrowStream(ctx, "SELECT 'abc'; SELECT 'abc'")
		assertNilF(t, err)
		assertTrueF(t, loader.TotalRows() > 0, "should return data")
	})
}
