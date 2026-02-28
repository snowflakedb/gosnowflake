package gosnowflake_test

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	sf "github.com/snowflakedb/gosnowflake/v2"
	"github.com/snowflakedb/gosnowflake/v2/arrowbatches"
)

func arrowTestRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	for {
		if _, err = os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		if !os.IsNotExist(err) {
			t.Fatalf("failed to stat go.mod in %q: %v", dir, err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repository root (no go.mod found)")
		}
		dir = parent
	}
}

func arrowTestReadPrivateKey(t *testing.T, path string) *rsa.PrivateKey {
	t.Helper()
	if !filepath.IsAbs(path) {
		path = filepath.Join(arrowTestRepoRoot(t), path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read private key file %q: %v", path, err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatalf("failed to decode PEM block from %q", path)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatalf("failed to parse private key from %q: %v", path, err)
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		t.Fatalf("private key in %q is not RSA (got %T)", path, key)
	}
	return rsaKey
}

// arrowTestConn manages a Snowflake connection for arrow batch tests.
type arrowTestConn struct {
	db   *sql.DB
	conn *sql.Conn
}

func openArrowTestConn(t *testing.T) *arrowTestConn {
	t.Helper()
	configParams := []*sf.ConfigParam{
		{Name: "Account", EnvName: "SNOWFLAKE_TEST_ACCOUNT", FailOnMissing: true},
		{Name: "User", EnvName: "SNOWFLAKE_TEST_USER", FailOnMissing: true},
		{Name: "Host", EnvName: "SNOWFLAKE_TEST_HOST", FailOnMissing: false},
		{Name: "Port", EnvName: "SNOWFLAKE_TEST_PORT", FailOnMissing: false},
		{Name: "Protocol", EnvName: "SNOWFLAKE_TEST_PROTOCOL", FailOnMissing: false},
		{Name: "Warehouse", EnvName: "SNOWFLAKE_TEST_WAREHOUSE", FailOnMissing: false},
	}
	isJWT := os.Getenv("SNOWFLAKE_TEST_AUTHENTICATOR") == "SNOWFLAKE_JWT"
	if !isJWT {
		configParams = append(configParams,
			&sf.ConfigParam{Name: "Password", EnvName: "SNOWFLAKE_TEST_PASSWORD", FailOnMissing: true},
		)
	}
	cfg, err := sf.GetConfigFromEnv(configParams)
	if err != nil {
		t.Fatalf("failed to get config from environment: %v", err)
	}
	if isJWT {
		privKeyPath := os.Getenv("SNOWFLAKE_TEST_PRIVATE_KEY")
		if privKeyPath == "" {
			t.Fatal("SNOWFLAKE_TEST_PRIVATE_KEY must be set for JWT authentication")
		}
		cfg.PrivateKey = arrowTestReadPrivateKey(t, privKeyPath)
		cfg.Authenticator = sf.AuthTypeJwt
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
	conn, err := db.Conn(context.Background())
	if err != nil {
		db.Close()
		t.Fatalf("failed to get connection: %v", err)
	}
	return &arrowTestConn{db: db, conn: conn}
}

func (tc *arrowTestConn) close() {
	tc.conn.Close()
	tc.db.Close()
}

func (tc *arrowTestConn) exec(t *testing.T, query string) {
	t.Helper()
	_, err := tc.conn.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatalf("exec %q failed: %v", query, err)
	}
}

func (tc *arrowTestConn) enableStructuredTypes(t *testing.T) {
	t.Helper()
	tc.exec(t, "ALTER SESSION SET ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE = true")
	tc.exec(t, "ALTER SESSION SET IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE = true")
}

func (tc *arrowTestConn) forceNativeArrow(t *testing.T) {
	t.Helper()
	tc.exec(t, "ALTER SESSION SET GO_QUERY_RESULT_FORMAT = ARROW")
	tc.exec(t, "ALTER SESSION SET ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT = true")
	tc.exec(t, "ALTER SESSION SET FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT = true")
}

func (tc *arrowTestConn) queryArrowBatches(t *testing.T, ctx context.Context, query string) ([]*arrowbatches.ArrowBatch, func()) {
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
	batches, err := arrowbatches.GetArrowBatches(sfRows)
	if err != nil {
		rows.Close()
		t.Fatalf("GetArrowBatches failed: %v", err)
	}
	if len(batches) == 0 {
		rows.Close()
		t.Fatal("expected at least one batch")
	}
	return batches, func() { rows.Close() }
}

func (tc *arrowTestConn) fetchFirst(t *testing.T, ctx context.Context, query string) ([]arrow.Record, func()) {
	t.Helper()
	batches, closeRows := tc.queryArrowBatches(t, ctx, query)
	records, err := batches[0].Fetch()
	if err != nil {
		closeRows()
		t.Fatalf("Fetch failed: %v", err)
	}
	if records == nil || len(*records) == 0 {
		closeRows()
		t.Fatal("expected at least one record")
	}
	return *records, closeRows
}

func equalIgnoringWhitespace(a, b string) bool {
	return strings.ReplaceAll(strings.ReplaceAll(a, " ", ""), "\n", "") ==
		strings.ReplaceAll(strings.ReplaceAll(b, " ", ""), "\n", "")
}

func TestStructuredTypeInArrowBatchesSimple(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	ctx := sf.WithArrowAllocator(arrowbatches.WithArrowBatches(context.Background()), pool)

	tc := openArrowTestConn(t)
	defer tc.close()
	tc.enableStructuredTypes(t)
	tc.forceNativeArrow(t)

	records, closeRows := tc.fetchFirst(t, ctx,
		"SELECT 1, {'s': 'some string'}::OBJECT(s VARCHAR)")
	defer closeRows()

	for _, record := range records {
		defer record.Release()
		if v := record.Column(0).(*array.Int8).Value(0); v != int8(1) {
			t.Errorf("expected column 0 = 1, got %v", v)
		}
		if v := record.Column(1).(*array.Struct).Field(0).(*array.String).Value(0); v != "some string" {
			t.Errorf("expected 'some string', got %q", v)
		}
	}
}

func TestStructuredTypeInArrowBatchesAllTypes(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	ctx := sf.WithArrowAllocator(arrowbatches.WithArrowBatches(context.Background()), pool)

	tc := openArrowTestConn(t)
	defer tc.close()
	tc.enableStructuredTypes(t)
	tc.forceNativeArrow(t)

	records, closeRows := tc.fetchFirst(t, ctx,
		"SELECT 1, {'s': 'some string', 'i': 1, 'time': '11:22:33'::TIME, 'date': '2024-04-16'::DATE, "+
			"'ltz': '2024-04-16 11:22:33'::TIMESTAMPLTZ, 'tz': '2025-04-16 22:33:11 +0100'::TIMESTAMPTZ, "+
			"'ntz': '2026-04-16 15:22:31'::TIMESTAMPNTZ}::OBJECT(s VARCHAR, i INTEGER, time TIME, date DATE, "+
			"ltz TIMESTAMPLTZ, tz TIMESTAMPTZ, ntz TIMESTAMPNTZ)")
	defer closeRows()

	for _, record := range records {
		defer record.Release()
		if v := record.Column(0).(*array.Int8).Value(0); v != int8(1) {
			t.Errorf("expected column 0 = 1, got %v", v)
		}
		st := record.Column(1).(*array.Struct)
		if v := st.Field(0).(*array.String).Value(0); v != "some string" {
			t.Errorf("expected 'some string', got %q", v)
		}
		if v := st.Field(1).(*array.Int64).Value(0); v != 1 {
			t.Errorf("expected i=1, got %v", v)
		}
		if v := st.Field(2).(*array.Time64).Value(0).ToTime(arrow.Nanosecond); !v.Equal(time.Date(1970, 1, 1, 11, 22, 33, 0, time.UTC)) {
			t.Errorf("expected time 11:22:33, got %v", v)
		}
		if v := st.Field(3).(*array.Date32).Value(0).ToTime(); !v.Equal(time.Date(2024, 4, 16, 0, 0, 0, 0, time.UTC)) {
			t.Errorf("expected date 2024-04-16, got %v", v)
		}
		if v := st.Field(4).(*array.Timestamp).Value(0).ToTime(arrow.Nanosecond); !v.Equal(time.Date(2024, 4, 16, 11, 22, 33, 0, time.UTC)) {
			t.Errorf("expected ltz 2024-04-16 11:22:33 UTC, got %v", v)
		}
		if v := st.Field(5).(*array.Timestamp).Value(0).ToTime(arrow.Nanosecond); !v.Equal(time.Date(2025, 4, 16, 21, 33, 11, 0, time.UTC)) {
			t.Errorf("expected tz 2025-04-16 21:33:11 UTC, got %v", v)
		}
		if v := st.Field(6).(*array.Timestamp).Value(0).ToTime(arrow.Nanosecond); !v.Equal(time.Date(2026, 4, 16, 15, 22, 31, 0, time.UTC)) {
			t.Errorf("expected ntz 2026-04-16 15:22:31, got %v", v)
		}
	}
}

func TestStructuredTypeInArrowBatchesWithTimestampOptionAndHigherPrecisionAndUtf8Validation(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	ctx := arrowbatches.WithUtf8Validation(
		sf.WithHigherPrecision(
			arrowbatches.WithTimestampOption(
				sf.WithArrowAllocator(arrowbatches.WithArrowBatches(context.Background()), pool),
				arrowbatches.UseOriginalTimestamp,
			),
		),
	)

	tc := openArrowTestConn(t)
	defer tc.close()
	tc.enableStructuredTypes(t)
	tc.forceNativeArrow(t)

	records, closeRows := tc.fetchFirst(t, ctx,
		"SELECT 1, {'i': 123, 'f': 12.34, 'n0': 321, 'n19': 1.5, 's': 'some string', "+
			"'bi': TO_BINARY('616263', 'HEX'), 'bool': true, 'time': '11:22:33', "+
			"'date': '2024-04-18', 'ntz': '2024-04-01 11:22:33', "+
			"'tz': '2024-04-02 11:22:33 +0100', 'ltz': '2024-04-03 11:22:33'}::"+
			"OBJECT(i INTEGER, f DOUBLE, n0 NUMBER(38, 0), n19 NUMBER(38, 19), "+
			"s VARCHAR, bi BINARY, bool BOOLEAN, time TIME, date DATE, "+
			"ntz TIMESTAMP_NTZ, tz TIMESTAMP_TZ, ltz TIMESTAMP_LTZ)")
	defer closeRows()

	for _, record := range records {
		defer record.Release()
		if v := record.Column(0).(*array.Int8).Value(0); v != int8(1) {
			t.Errorf("expected column 0 = 1, got %v", v)
		}
		st := record.Column(1).(*array.Struct)
		if v := st.Field(0).(*array.Decimal128).Value(0).LowBits(); v != uint64(123) {
			t.Errorf("expected i=123, got %v", v)
		}
		if v := st.Field(1).(*array.Float64).Value(0); v != 12.34 {
			t.Errorf("expected f=12.34, got %v", v)
		}
		if v := st.Field(2).(*array.Decimal128).Value(0).LowBits(); v != uint64(321) {
			t.Errorf("expected n0=321, got %v", v)
		}
		if v := st.Field(3).(*array.Decimal128).Value(0).LowBits(); v != uint64(15000000000000000000) {
			t.Errorf("expected n19=15000000000000000000, got %v", v)
		}
		if v := st.Field(4).(*array.String).Value(0); v != "some string" {
			t.Errorf("expected 'some string', got %q", v)
		}
		if v := st.Field(5).(*array.Binary).Value(0); !reflect.DeepEqual(v, []byte{'a', 'b', 'c'}) {
			t.Errorf("expected 'abc' binary, got %v", v)
		}
		if v := st.Field(6).(*array.Boolean).Value(0); v != true {
			t.Errorf("expected true, got %v", v)
		}
		if v := st.Field(7).(*array.Time64).Value(0).ToTime(arrow.Nanosecond); !v.Equal(time.Date(1970, 1, 1, 11, 22, 33, 0, time.UTC)) {
			t.Errorf("expected time 11:22:33, got %v", v)
		}
		if v := st.Field(8).(*array.Date32).Value(0).ToTime(); !v.Equal(time.Date(2024, 4, 18, 0, 0, 0, 0, time.UTC)) {
			t.Errorf("expected date 2024-04-18, got %v", v)
		}
		// With UseOriginalTimestamp, timestamps remain as raw structs (epoch + fraction)
		if v := st.Field(9).(*array.Struct).Field(0).(*array.Int64).Value(0); v != int64(1711970553) {
			t.Errorf("expected ntz epoch=1711970553, got %v", v)
		}
		if v := st.Field(9).(*array.Struct).Field(1).(*array.Int32).Value(0); v != int32(0) {
			t.Errorf("expected ntz fraction=0, got %v", v)
		}
		if v := st.Field(10).(*array.Struct).Field(0).(*array.Int64).Value(0); v != int64(1712053353) {
			t.Errorf("expected tz epoch=1712053353, got %v", v)
		}
		if v := st.Field(10).(*array.Struct).Field(1).(*array.Int32).Value(0); v != int32(0) {
			t.Errorf("expected tz fraction=0, got %v", v)
		}
		if v := st.Field(11).(*array.Struct).Field(0).(*array.Int64).Value(0); v != int64(1712143353) {
			t.Errorf("expected ltz epoch=1712143353, got %v", v)
		}
		if v := st.Field(11).(*array.Struct).Field(1).(*array.Int32).Value(0); v != int32(0) {
			t.Errorf("expected ltz fraction=0, got %v", v)
		}
	}
}

func TestStructuredTypeInArrowBatchesWithEmbeddedObject(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	ctx := sf.WithArrowAllocator(arrowbatches.WithArrowBatches(context.Background()), pool)

	tc := openArrowTestConn(t)
	defer tc.close()
	tc.enableStructuredTypes(t)
	tc.forceNativeArrow(t)

	records, closeRows := tc.fetchFirst(t, ctx,
		"SELECT {'o': {'s': 'some string'}}::OBJECT(o OBJECT(s VARCHAR))")
	defer closeRows()

	for _, record := range records {
		defer record.Release()
		if v := record.Column(0).(*array.Struct).Field(0).(*array.Struct).Field(0).(*array.String).Value(0); v != "some string" {
			t.Errorf("expected 'some string', got %q", v)
		}
	}
}

func TestStructuredTypeInArrowBatchesAsNull(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	ctx := sf.WithArrowAllocator(arrowbatches.WithArrowBatches(context.Background()), pool)

	tc := openArrowTestConn(t)
	defer tc.close()
	tc.enableStructuredTypes(t)
	tc.forceNativeArrow(t)

	records, closeRows := tc.fetchFirst(t, ctx,
		"SELECT {'s': 'some string'}::OBJECT(s VARCHAR) UNION SELECT null ORDER BY 1")
	defer closeRows()

	for _, record := range records {
		defer record.Release()
		if record.Column(0).IsNull(0) {
			t.Error("expected first row to be non-null")
		}
		if !record.Column(0).IsNull(1) {
			t.Error("expected second row to be null")
		}
	}
}

func TestStructuredArrayInArrowBatches(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	ctx := sf.WithArrowAllocator(arrowbatches.WithArrowBatches(context.Background()), pool)

	tc := openArrowTestConn(t)
	defer tc.close()
	tc.enableStructuredTypes(t)
	tc.forceNativeArrow(t)

	records, closeRows := tc.fetchFirst(t, ctx,
		"SELECT [1, 2, 3]::ARRAY(INTEGER) UNION SELECT [4, 5, 6]::ARRAY(INTEGER) ORDER BY 1")
	defer closeRows()

	record := records[0]
	defer record.Release()

	listCol := record.Column(0).(*array.List)
	vals := listCol.ListValues().(*array.Int64)
	expectedVals := []int64{1, 2, 3, 4, 5, 6}
	for i, exp := range expectedVals {
		if v := vals.Value(i); v != exp {
			t.Errorf("list value[%d]: expected %d, got %d", i, exp, v)
		}
	}
	expectedOffsets := []int32{0, 3, 6}
	for i, exp := range expectedOffsets {
		if v := listCol.Offsets()[i]; v != exp {
			t.Errorf("offset[%d]: expected %d, got %d", i, exp, v)
		}
	}
}

func TestStructuredMapInArrowBatches(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	ctx := sf.WithArrowAllocator(arrowbatches.WithArrowBatches(context.Background()), pool)

	tc := openArrowTestConn(t)
	defer tc.close()
	tc.enableStructuredTypes(t)
	tc.forceNativeArrow(t)

	records, closeRows := tc.fetchFirst(t, ctx,
		"SELECT {'a': 'b', 'c': 'd'}::MAP(VARCHAR, VARCHAR)")
	defer closeRows()

	for _, record := range records {
		defer record.Release()
		m := record.Column(0).(*array.Map)
		keys := m.Keys().(*array.String)
		items := m.Items().(*array.String)
		if v := keys.Value(0); v != "a" {
			t.Errorf("expected key 'a', got %q", v)
		}
		if v := keys.Value(1); v != "c" {
			t.Errorf("expected key 'c', got %q", v)
		}
		if v := items.Value(0); v != "b" {
			t.Errorf("expected item 'b', got %q", v)
		}
		if v := items.Value(1); v != "d" {
			t.Errorf("expected item 'd', got %q", v)
		}
	}
}

func TestSelectingNullObjectsInArrowBatches(t *testing.T) {
	testcases := []string{
		"select null::object(v VARCHAR)",
		"select null::object",
	}

	tc := openArrowTestConn(t)
	defer tc.close()
	tc.enableStructuredTypes(t)

	for _, query := range testcases {
		t.Run(query, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			ctx := sf.WithArrowAllocator(arrowbatches.WithArrowBatches(context.Background()), pool)

			records, closeRows := tc.fetchFirst(t, ctx, query)
			defer closeRows()

			for _, record := range records {
				defer record.Release()
				if record.NumRows() != 1 {
					t.Fatalf("wrong number of rows: expected 1, got %d", record.NumRows())
				}
				if record.NumCols() != 1 {
					t.Fatalf("wrong number of cols: expected 1, got %d", record.NumCols())
				}
				if !record.Column(0).IsNull(0) {
					t.Error("expected null value")
				}
			}
		})
	}
}

func TestSelectingSemistructuredTypesInArrowBatches(t *testing.T) {
	testcases := []struct {
		name               string
		query              string
		expected           string
		withUtf8Validation bool
	}{
		{
			name:               "semistructured object with utf8 validation",
			withUtf8Validation: true,
			expected:           `{"s":"someString"}`,
			query:              "SELECT {'s':'someString'}::OBJECT",
		},
		{
			name:               "semistructured object without utf8 validation",
			withUtf8Validation: false,
			expected:           `{"s":"someString"}`,
			query:              "SELECT {'s':'someString'}::OBJECT",
		},
		{
			name:               "semistructured array without utf8 validation",
			withUtf8Validation: false,
			expected:           `[1,2,3]`,
			query:              "SELECT [1, 2, 3]::ARRAY",
		},
		{
			name:               "semistructured array with utf8 validation",
			withUtf8Validation: true,
			expected:           `[1,2,3]`,
			query:              "SELECT [1, 2, 3]::ARRAY",
		},
	}

	tc := openArrowTestConn(t)
	defer tc.close()

	for _, tc2 := range testcases {
		t.Run(tc2.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			ctx := sf.WithArrowAllocator(arrowbatches.WithArrowBatches(context.Background()), pool)
			if tc2.withUtf8Validation {
				ctx = arrowbatches.WithUtf8Validation(ctx)
			}

			records, closeRows := tc.fetchFirst(t, ctx, tc2.query)
			defer closeRows()

			for _, record := range records {
				defer record.Release()
				if record.NumCols() != 1 {
					t.Fatalf("unexpected number of columns: %d", record.NumCols())
				}
				if record.NumRows() != 1 {
					t.Fatalf("unexpected number of rows: %d", record.NumRows())
				}
				stringCol, ok := record.Column(0).(*array.String)
				if !ok {
					t.Fatalf("wrong type for column, expected string, got %T", record.Column(0))
				}
				if !equalIgnoringWhitespace(stringCol.Value(0), tc2.expected) {
					t.Errorf("expected %q, got %q", tc2.expected, stringCol.Value(0))
				}
			}
		})
	}
}
