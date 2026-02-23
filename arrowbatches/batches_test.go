package arrowbatches

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	sf "github.com/snowflakedb/gosnowflake/v2"
	ia "github.com/snowflakedb/gosnowflake/v2/internal/arrow"
)

var configParams = []*sf.ConfigParam{
	{Name: "Account", EnvName: "SNOWFLAKE_TEST_ACCOUNT", FailOnMissing: true},
	{Name: "User", EnvName: "SNOWFLAKE_TEST_USER", FailOnMissing: true},
	{Name: "Password", EnvName: "SNOWFLAKE_TEST_PASSWORD", FailOnMissing: true},
	{Name: "Host", EnvName: "SNOWFLAKE_TEST_HOST", FailOnMissing: false},
	{Name: "Port", EnvName: "SNOWFLAKE_TEST_PORT", FailOnMissing: false},
	{Name: "Protocol", EnvName: "SNOWFLAKE_TEST_PROTOCOL", FailOnMissing: false},
	{Name: "Warehouse", EnvName: "SNOWFLAKE_TEST_WAREHOUSE", FailOnMissing: false},
}

// testConn holds a reusable database connection for running multiple queries.
type testConn struct {
	db   *sql.DB
	conn *sql.Conn
}

func openTestConn(ctx context.Context, t *testing.T) *testConn {
	t.Helper()
	cfg, err := sf.GetConfigFromEnv(configParams)
	if err != nil {
		t.Skipf("Snowflake config not available: %v", err)
	}
	tz := "UTC"
	if cfg.Params == nil {
		cfg.Params = make(map[string]*string)
	}
	cfg.Params["timezone"] = &tz
	dsn, err := sf.DSN(cfg)
	if err != nil {
		t.Fatalf("failed to create DSN: %v", err)
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		t.Fatalf("failed to get connection: %v", err)
	}
	return &testConn{db: db, conn: conn}
}

func (tc *testConn) close() {
	tc.conn.Close()
	tc.db.Close()
}

// queryRows executes a query on the existing connection and returns
// SnowflakeRows plus a function to close just the rows.
func (tc *testConn) queryRows(ctx context.Context, t *testing.T, query string) (sf.SnowflakeRows, func()) {
	t.Helper()
	var rows driver.Rows
	var err error
	err = tc.conn.Raw(func(x interface{}) error {
		queryer, ok := x.(driver.QueryerContext)
		if !ok {
			return fmt.Errorf("connection does not implement QueryerContext")
		}
		rows, err = queryer.QueryContext(ctx, query, nil)
		return err
	})
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	sfRows, ok := rows.(sf.SnowflakeRows)
	if !ok {
		rows.Close()
		t.Fatalf("rows do not implement SnowflakeRows")
	}
	return sfRows, func() { rows.Close() }
}

// queryRawRows is a convenience wrapper that opens a new connection,
// runs a single query, and returns SnowflakeRows with a full cleanup.
func queryRawRows(ctx context.Context, t *testing.T, query string) (sf.SnowflakeRows, func()) {
	t.Helper()
	tc := openTestConn(ctx, t)
	sfRows, closeRows := tc.queryRows(ctx, t, query)
	return sfRows, func() {
		closeRows()
		tc.close()
	}
}

func TestGetArrowBatches(t *testing.T) {
	ctx := WithArrowBatches(context.Background())

	sfRows, cleanup := queryRawRows(ctx, t, "SELECT 1 AS num, 'hello' AS str")
	defer cleanup()

	batches, err := GetArrowBatches(sfRows)
	if err != nil {
		t.Fatalf("GetArrowBatches failed: %v", err)
	}
	if len(batches) == 0 {
		t.Fatal("expected at least one batch")
	}

	records, err := batches[0].Fetch()
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if records == nil || len(*records) == 0 {
		t.Fatal("expected at least one record")
	}

	rec := (*records)[0]
	defer rec.Release()

	if rec.NumCols() != 2 {
		t.Fatalf("expected 2 columns, got %d", rec.NumCols())
	}
	if rec.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", rec.NumRows())
	}
}

func TestGetArrowBatchesHighPrecision(t *testing.T) {
	ctx := sf.WithHigherPrecision(WithArrowBatches(context.Background()))

	sfRows, cleanup := queryRawRows(ctx, t, "SELECT '0.1'::DECIMAL(38, 19) AS c")
	defer cleanup()

	batches, err := GetArrowBatches(sfRows)
	if err != nil {
		t.Fatalf("GetArrowBatches failed: %v", err)
	}
	if len(batches) == 0 {
		t.Fatal("expected at least one batch")
	}

	records, err := batches[0].Fetch()
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if records == nil || len(*records) == 0 {
		t.Fatal("expected at least one record")
	}

	rec := (*records)[0]
	defer rec.Release()

	strVal := rec.Column(0).ValueStr(0)
	expected := "1000000000000000000"
	if strVal != expected {
		t.Fatalf("expected %q, got %q", expected, strVal)
	}
}

func TestGetArrowBatchesLargeResultSet(t *testing.T) {
	numrows := 3000
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)

	ctx := sf.WithArrowAllocator(WithArrowBatches(context.Background()), pool)

	query := fmt.Sprintf("SELECT SEQ8(), RANDSTR(1000, RANDOM()) FROM TABLE(GENERATOR(ROWCOUNT=>%v))", numrows)
	sfRows, cleanup := queryRawRows(ctx, t, query)
	defer cleanup()

	batches, err := GetArrowBatches(sfRows)
	if err != nil {
		t.Fatalf("GetArrowBatches failed: %v", err)
	}
	if len(batches) == 0 {
		t.Fatal("expected at least one batch")
	}

	maxWorkers := 10
	type count struct {
		mu  sync.Mutex
		val int
	}
	cnt := &count{}
	var wg sync.WaitGroup
	work := make(chan int, len(batches))

	for w := 0; w < maxWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				recs, fetchErr := batches[i].Fetch()
				if fetchErr != nil {
					t.Errorf("Fetch failed for batch %d: %v", i, fetchErr)
					return
				}
				for _, r := range *recs {
					cnt.mu.Lock()
					cnt.val += int(r.NumRows())
					cnt.mu.Unlock()
					r.Release()
				}
			}
		}()
	}
	for i := range batches {
		work <- i
	}
	close(work)
	wg.Wait()

	if cnt.val != numrows {
		t.Fatalf("row count mismatch: expected %d, got %d", numrows, cnt.val)
	}
}

func TestGetArrowBatchesWithTimestampOption(t *testing.T) {
	ctx := WithTimestampOption(
		WithArrowBatches(context.Background()),
		UseOriginalTimestamp,
	)

	sfRows, cleanup := queryRawRows(ctx, t, "SELECT TO_TIMESTAMP_NTZ('2024-01-15 13:45:30.123456789') AS ts")
	defer cleanup()

	batches, err := GetArrowBatches(sfRows)
	if err != nil {
		t.Fatalf("GetArrowBatches failed: %v", err)
	}
	if len(batches) == 0 {
		t.Fatal("expected at least one batch")
	}

	records, err := batches[0].Fetch()
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if records == nil || len(*records) == 0 {
		t.Fatal("expected at least one record")
	}

	rec := (*records)[0]
	defer rec.Release()

	if rec.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", rec.NumRows())
	}
	if rec.NumCols() != 1 {
		t.Fatalf("expected 1 column, got %d", rec.NumCols())
	}
}

func TestGetArrowBatchesJSONResponseError(t *testing.T) {
	ctx := WithArrowBatches(context.Background())

	cfg, err := sf.GetConfigFromEnv([]*sf.ConfigParam{
		{Name: "Account", EnvName: "SNOWFLAKE_TEST_ACCOUNT", FailOnMissing: true},
		{Name: "User", EnvName: "SNOWFLAKE_TEST_USER", FailOnMissing: true},
		{Name: "Password", EnvName: "SNOWFLAKE_TEST_PASSWORD", FailOnMissing: true},
		{Name: "Host", EnvName: "SNOWFLAKE_TEST_HOST", FailOnMissing: false},
		{Name: "Port", EnvName: "SNOWFLAKE_TEST_PORT", FailOnMissing: false},
		{Name: "Protocol", EnvName: "SNOWFLAKE_TEST_PROTOCOL", FailOnMissing: false},
	})
	if err != nil {
		t.Skipf("Snowflake config not available: %v", err)
	}

	dsn, err := sf.DSN(cfg)
	if err != nil {
		t.Fatalf("failed to create DSN: %v", err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}
	defer conn.Close()

	// Force JSON response format on this connection
	_, err = conn.ExecContext(ctx, "ALTER SESSION SET GO_QUERY_RESULT_FORMAT = json")
	if err != nil {
		t.Fatalf("failed to set JSON format: %v", err)
	}

	var rows driver.Rows
	err = conn.Raw(func(x interface{}) error {
		queryer, ok := x.(driver.QueryerContext)
		if !ok {
			return fmt.Errorf("connection does not implement QueryerContext")
		}
		rows, err = queryer.QueryContext(ctx, "SELECT 'hello'", nil)
		return err
	})
	if err != nil {
		t.Fatalf("failed to execute query: %v", err)
	}
	defer rows.Close()

	sfRows, ok := rows.(sf.SnowflakeRows)
	if !ok {
		t.Fatal("rows do not implement SnowflakeRows")
	}

	_, err = GetArrowBatches(sfRows)
	if err == nil {
		t.Fatal("expected error when using arrow batches with JSON response")
	}

	var se *sf.SnowflakeError
	if !errors.As(err, &se) {
		t.Fatalf("expected SnowflakeError, got %T: %v", err, err)
	}
	if se.Number != sf.ErrNonArrowResponseInArrowBatches {
		t.Fatalf("expected error code %d, got %d", sf.ErrNonArrowResponseInArrowBatches, se.Number)
	}
}

func TestTimestampConversionDistantDates(t *testing.T) {
	timestamps := [2]string{
		"9999-12-12 23:59:59.999999999",
		"0001-01-01 00:00:00.000000000",
	}
	tsTypes := [3]string{"TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}

	precisions := []struct {
		name        string
		option      ia.TimestampOption
		unit        arrow.TimeUnit
		expectError bool
	}{
		{"second", UseSecondTimestamp, arrow.Second, false},
		{"millisecond", UseMillisecondTimestamp, arrow.Millisecond, false},
		{"microsecond", UseMicrosecondTimestamp, arrow.Microsecond, false},
		{"nanosecond", UseNanosecondTimestamp, arrow.Nanosecond, true},
	}

	for _, prec := range precisions {
		t.Run(prec.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)

			ctx := sf.WithArrowAllocator(
				WithTimestampOption(WithArrowBatches(context.Background()), prec.option),
				pool,
			)

			tc := openTestConn(ctx, t)
			defer tc.close()

			for _, tsStr := range timestamps {
				for _, tp := range tsTypes {
					for scale := 0; scale <= 9; scale++ {
						t.Run(tp+"("+strconv.Itoa(scale)+")_"+tsStr, func(t *testing.T) {
							query := fmt.Sprintf("SELECT '%s'::%s(%v)", tsStr, tp, scale)
							sfRows, closeRows := tc.queryRows(ctx, t, query)
							defer closeRows()

							batches, err := GetArrowBatches(sfRows)
							if err != nil {
								t.Fatalf("GetArrowBatches failed: %v", err)
							}
							if len(batches) == 0 {
								t.Fatal("expected at least one batch")
							}

							records, err := batches[0].Fetch()

							if prec.expectError {
								expectedError := "Cannot convert timestamp"
								if err == nil {
									t.Fatalf("no error, expected: %v", expectedError)
								}
								if !strings.Contains(err.Error(), expectedError) {
									t.Fatalf("improper error, expected: %v, got: %v", expectedError, err.Error())
								}
								return
							}

							if err != nil {
								t.Fatalf("Fetch failed: %v", err)
							}
							if records == nil || len(*records) == 0 {
								t.Fatal("expected at least one record")
							}

							rec := (*records)[0]
							defer rec.Release()

							actual := rec.Column(0).(*array.Timestamp).TimestampValues()[0]
							actualYear := actual.ToTime(prec.unit).Year()

							ts, err := time.Parse("2006-01-02 15:04:05", tsStr)
							if err != nil {
								t.Fatalf("failed to parse time: %v", err)
							}
							exp := ts.Truncate(time.Duration(math.Pow10(9 - scale)))

							if actualYear != exp.Year() {
								t.Fatalf("unexpected year, expected: %v, got: %v", exp.Year(), actualYear)
							}
						})
					}
				}
			}
		})
	}
}

func TestTimestampConversionWithOriginalTimestamp(t *testing.T) {
	timestamps := [3]string{
		"2000-10-10 10:10:10.123456789",
		"9999-12-12 23:59:59.999999999",
		"0001-01-01 00:00:00.000000000",
	}
	tsTypes := [3]string{"TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}

	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)

	ctx := sf.WithArrowAllocator(
		WithTimestampOption(WithArrowBatches(context.Background()), UseOriginalTimestamp),
		pool,
	)

	tc := openTestConn(ctx, t)
	defer tc.close()

	for _, tsStr := range timestamps {
		ts, err := time.Parse("2006-01-02 15:04:05", tsStr)
		if err != nil {
			t.Fatalf("failed to parse time: %v", err)
		}
		for _, tp := range tsTypes {
			for scale := 0; scale <= 9; scale++ {
				t.Run(tp+"("+strconv.Itoa(scale)+")_"+tsStr, func(t *testing.T) {
					query := fmt.Sprintf("SELECT '%s'::%s(%v)", tsStr, tp, scale)
					sfRows, closeRows := tc.queryRows(ctx, t, query)
					defer closeRows()

					batches, err := GetArrowBatches(sfRows)
					if err != nil {
						t.Fatalf("GetArrowBatches failed: %v", err)
					}
					if len(batches) != 1 {
						t.Fatalf("expected 1 batch, got %d", len(batches))
					}

					records, err := batches[0].Fetch()
					if err != nil {
						t.Fatalf("Fetch failed: %v", err)
					}
					if records == nil || len(*records) == 0 {
						t.Fatal("expected at least one record")
					}

					exp := ts.Truncate(time.Duration(math.Pow10(9 - scale)))
					for _, r := range *records {
						defer r.Release()
						act := batches[0].ArrowSnowflakeTimestampToTime(r, 0, 0)
						if act == nil {
							t.Fatalf("unexpected nil, expected: %v", exp)
						} else if !exp.Equal(*act) {
							t.Fatalf("unexpected result, expected: %v, got: %v", exp, *act)
						}
					}
				})
			}
		}
	}
}
