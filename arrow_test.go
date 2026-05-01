package gosnowflake

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	ia "github.com/snowflakedb/gosnowflake/v2/internal/arrow"
	"github.com/snowflakedb/gosnowflake/v2/internal/query"
)

func TestArrowBatchDataProvider(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		ctx := ia.EnableArrowBatches(context.Background())
		query := "select '0.1':: DECIMAL(38, 19) as c"

		var rows driver.Rows
		var err error

		err = dbt.conn.Raw(func(x any) error {
			queryer, implementsQueryContext := x.(driver.QueryerContext)
			assertTrueF(t, implementsQueryContext, "snowflake connection driver does not implement queryerContext")

			rows, err = queryer.QueryContext(ctx, query, nil)
			return err
		})

		assertNilF(t, err, "error running select query")

		sfRows, isSfRows := rows.(SnowflakeRows)
		assertTrueF(t, isSfRows, "rows should be snowflakeRows")

		provider, isProvider := sfRows.(ia.BatchDataProvider)
		assertTrueF(t, isProvider, "rows should implement BatchDataProvider")

		info, err := provider.GetArrowBatches()
		assertNilF(t, err, "error getting arrow batch data")
		assertNotEqualF(t, len(info.Batches), 0, "should have at least one batch")

		// Verify raw records are available for the first batch
		batch := info.Batches[0]
		assertNotNilF(t, batch.Records, "first batch should have pre-decoded records")

		records := *batch.Records
		assertNotEqualF(t, len(records), 0, "should have at least one record")

		// Verify column 0 has data (raw decimal value)
		strVal := records[0].Column(0).ValueStr(0)
		assertTrueF(t, len(strVal) > 0, fmt.Sprintf("column should have a value, got: %s", strVal))
	})
}

func TestArrowBigInt(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		testcases := []struct {
			num  string
			prec int
			sc   int
		}{
			{"10000000000000000000000000000000000000", 38, 0},
			{"-10000000000000000000000000000000000000", 38, 0},
			{"12345678901234567890123456789012345678", 38, 0}, // #pragma: allowlist secret
			{"-12345678901234567890123456789012345678", 38, 0},
			{"99999999999999999999999999999999999999", 38, 0},
			{"-99999999999999999999999999999999999999", 38, 0},
		}

		for _, tc := range testcases {
			rows := dbt.mustQueryContext(WithHigherPrecision(context.Background()),
				fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
			if !rows.Next() {
				dbt.Error("failed to query")
			}
			defer rows.Close()
			var v *big.Int
			if err := rows.Scan(&v); err != nil {
				dbt.Errorf("failed to scan. %#v", err)
			}

			b, ok := new(big.Int).SetString(tc.num, 10)
			if !ok {
				dbt.Errorf("failed to convert %v big.Int.", tc.num)
			}
			if v.Cmp(b) != 0 {
				dbt.Errorf("big.Int value mismatch: expected %v, got %v", b, v)
			}
		}
	})
}

func TestArrowBigFloat(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		testcases := []struct {
			num  string
			prec int
			sc   int
		}{
			{"1.23", 30, 2},
			{"1.0000000000000000000000000000000000000", 38, 37},
			{"-1.0000000000000000000000000000000000000", 38, 37},
			{"1.2345678901234567890123456789012345678", 38, 37},
			{"-1.2345678901234567890123456789012345678", 38, 37},
			{"9.9999999999999999999999999999999999999", 38, 37},
			{"-9.9999999999999999999999999999999999999", 38, 37},
		}

		for _, tc := range testcases {
			rows := dbt.mustQueryContext(WithHigherPrecision(context.Background()),
				fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
			if !rows.Next() {
				dbt.Error("failed to query")
			}
			defer rows.Close()
			var v *big.Float
			if err := rows.Scan(&v); err != nil {
				dbt.Errorf("failed to scan. %#v", err)
			}

			prec := v.Prec()
			b, ok := new(big.Float).SetPrec(prec).SetString(tc.num)
			if !ok {
				dbt.Errorf("failed to convert %v to big.Float.", tc.num)
			}
			if v.Cmp(b) != 0 {
				dbt.Errorf("big.Float value mismatch: expected %v, got %v", b, v)
			}
		}
	})
}

func TestArrowIntPrecision(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec(forceJSON)

		intTestcases := []struct {
			num  string
			prec int
			sc   int
		}{
			{"10000000000000000000000000000000000000", 38, 0},
			{"-10000000000000000000000000000000000000", 38, 0},
			{"12345678901234567890123456789012345678", 38, 0}, // pragma: allowlist secret
			{"-12345678901234567890123456789012345678", 38, 0},
			{"99999999999999999999999999999999999999", 38, 0},
			{"-99999999999999999999999999999999999999", 38, 0},
		}

		t.Run("arrow_disabled_scan_int64", func(t *testing.T) {
			for _, tc := range intTestcases {
				rows := dbt.mustQuery(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
				defer rows.Close()
				if !rows.Next() {
					t.Error("failed to query")
				}
				var v int64
				if err := rows.Scan(&v); err == nil {
					t.Error("should fail to scan")
				}
			}
		})
		t.Run("arrow_disabled_scan_string", func(t *testing.T) {
			for _, tc := range intTestcases {
				rows := dbt.mustQuery(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
				defer rows.Close()
				if !rows.Next() {
					t.Error("failed to query")
				}
				var v string
				if err := rows.Scan(&v); err != nil {
					t.Errorf("failed to scan. %#v", err)
				}
				if v != tc.num {
					t.Errorf("string value mismatch: expected %v, got %v", tc.num, v)
				}
			}
		})

		dbt.mustExec(forceARROW)

		t.Run("arrow_enabled_scan_big_int", func(t *testing.T) {
			for _, tc := range intTestcases {
				rows := dbt.mustQuery(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
				defer rows.Close()
				if !rows.Next() {
					t.Error("failed to query")
				}
				var v string
				if err := rows.Scan(&v); err != nil {
					t.Errorf("failed to scan. %#v", err)
				}
				if !strings.EqualFold(v, tc.num) {
					t.Errorf("int value mismatch: expected %v, got %v", tc.num, v)
				}
			}
		})
		t.Run("arrow_high_precision_enabled_scan_big_int", func(t *testing.T) {
			for _, tc := range intTestcases {
				rows := dbt.mustQueryContext(WithHigherPrecision(context.Background()), fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
				defer rows.Close()
				if !rows.Next() {
					t.Error("failed to query")
				}
				var v *big.Int
				if err := rows.Scan(&v); err != nil {
					t.Errorf("failed to scan. %#v", err)
				}

				b, ok := new(big.Int).SetString(tc.num, 10)
				if !ok {
					t.Errorf("failed to convert %v big.Int.", tc.num)
				}
				if v.Cmp(b) != 0 {
					t.Errorf("big.Int value mismatch: expected %v, got %v", b, v)
				}
			}
		})
	})
}

// TestArrowFloatPrecision tests the different variable types allowed in the
// rows.Scan() method. Note that for lower precision types we do not attempt
// to check the value as precision could be lost.
func TestArrowFloatPrecision(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec(forceJSON)

		fltTestcases := []struct {
			num  string
			prec int
			sc   int
		}{
			{"1.23", 30, 2},
			{"1.0000000000000000000000000000000000000", 38, 37},
			{"-1.0000000000000000000000000000000000000", 38, 37},
			{"1.2345678901234567890123456789012345678", 38, 37},
			{"-1.2345678901234567890123456789012345678", 38, 37},
			{"9.9999999999999999999999999999999999999", 38, 37},
			{"-9.9999999999999999999999999999999999999", 38, 37},
		}

		t.Run("arrow_disabled_scan_float64", func(t *testing.T) {
			for _, tc := range fltTestcases {
				rows := dbt.mustQuery(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
				defer rows.Close()
				if !rows.Next() {
					t.Error("failed to query")
				}
				var v float64
				if err := rows.Scan(&v); err != nil {
					t.Errorf("failed to scan. %#v", err)
				}
			}
		})
		t.Run("arrow_disabled_scan_float32", func(t *testing.T) {
			for _, tc := range fltTestcases {
				rows := dbt.mustQuery(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
				defer rows.Close()
				if !rows.Next() {
					t.Error("failed to query")
				}
				var v float32
				if err := rows.Scan(&v); err != nil {
					t.Errorf("failed to scan. %#v", err)
				}
			}
		})
		t.Run("arrow_disabled_scan_string", func(t *testing.T) {
			for _, tc := range fltTestcases {
				rows := dbt.mustQuery(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
				defer rows.Close()
				if !rows.Next() {
					t.Error("failed to query")
				}
				var v string
				if err := rows.Scan(&v); err != nil {
					t.Errorf("failed to scan. %#v", err)
				}
				if !strings.EqualFold(v, tc.num) {
					t.Errorf("int value mismatch: expected %v, got %v", tc.num, v)
				}
			}
		})

		dbt.mustExec(forceARROW)

		t.Run("arrow_enabled_scan_float64", func(t *testing.T) {
			for _, tc := range fltTestcases {
				rows := dbt.mustQuery(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
				defer rows.Close()
				if !rows.Next() {
					t.Error("failed to query")
				}
				var v float64
				if err := rows.Scan(&v); err != nil {
					t.Errorf("failed to scan. %#v", err)
				}
			}
		})
		t.Run("arrow_enabled_scan_float32", func(t *testing.T) {
			for _, tc := range fltTestcases {
				rows := dbt.mustQuery(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
				defer rows.Close()
				if !rows.Next() {
					t.Error("failed to query")
				}
				var v float32
				if err := rows.Scan(&v); err != nil {
					t.Errorf("failed to scan. %#v", err)
				}
			}
		})
		t.Run("arrow_enabled_scan_string", func(t *testing.T) {
			for _, tc := range fltTestcases {
				rows := dbt.mustQuery(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
				defer rows.Close()
				if !rows.Next() {
					t.Error("failed to query")
				}
				var v string
				if err := rows.Scan(&v); err != nil {
					t.Errorf("failed to scan. %#v", err)
				}
				if v != tc.num {
					t.Errorf("string value mismatch: expected %v, got %v", tc.num, v)
				}
			}
		})
		t.Run("arrow_high_precision_enabled_scan_big_float", func(t *testing.T) {
			for _, tc := range fltTestcases {
				rows := dbt.mustQueryContext(WithHigherPrecision(context.Background()), fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
				defer rows.Close()
				if !rows.Next() {
					t.Error("failed to query")
				}
				var v *big.Float
				if err := rows.Scan(&v); err != nil {
					t.Errorf("failed to scan. %#v", err)
				}

				prec := v.Prec()
				b, ok := new(big.Float).SetPrec(prec).SetString(tc.num)
				if !ok {
					t.Errorf("failed to convert %v to big.Float.", tc.num)
				}
				if v.Cmp(b) != 0 {
					t.Errorf("big.Float value mismatch: expected %v, got %v", b, v)
				}
			}
		})
	})
}

func TestArrowTimePrecision(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE t (col5 TIME(5), col6 TIME(6), col7 TIME(7), col8 TIME(8));")
		defer dbt.mustExec("DROP TABLE IF EXISTS t")
		dbt.mustExec("INSERT INTO t VALUES ('23:59:59.99999', '23:59:59.999999', '23:59:59.9999999', '23:59:59.99999999');")

		rows := dbt.mustQuery("select * from t")
		defer rows.Close()
		var c5, c6, c7, c8 time.Time
		for rows.Next() {
			if err := rows.Scan(&c5, &c6, &c7, &c8); err != nil {
				t.Errorf("values were not scanned: %v", err)
			}
		}

		nano := 999999990
		expected := time.Time{}.Add(23*time.Hour + 59*time.Minute + 59*time.Second + 99*time.Millisecond)
		if c8.Unix() != expected.Unix() || c8.Nanosecond() != nano {
			t.Errorf("the value did not match. expected: %v, got: %v", expected, c8)
		}
		if c7.Unix() != expected.Unix() || c7.Nanosecond() != nano-(nano%1e2) {
			t.Errorf("the value did not match. expected: %v, got: %v", expected, c7)
		}
		if c6.Unix() != expected.Unix() || c6.Nanosecond() != nano-(nano%1e3) {
			t.Errorf("the value did not match. expected: %v, got: %v", expected, c6)
		}
		if c5.Unix() != expected.Unix() || c5.Nanosecond() != nano-(nano%1e4) {
			t.Errorf("the value did not match. expected: %v, got: %v", expected, c5)
		}

		dbt.mustExec(`CREATE TABLE t_ntz (
		  col1 TIMESTAMP_NTZ(1),
		  col2 TIMESTAMP_NTZ(2),
		  col3 TIMESTAMP_NTZ(3),
		  col4 TIMESTAMP_NTZ(4),
		  col5 TIMESTAMP_NTZ(5),
		  col6 TIMESTAMP_NTZ(6),
		  col7 TIMESTAMP_NTZ(7),
		  col8 TIMESTAMP_NTZ(8)
		);`)
		defer dbt.mustExec("DROP TABLE IF EXISTS t_ntz")
		dbt.mustExec(`INSERT INTO t_ntz VALUES (
		  '9999-12-31T23:59:59.9',
		  '9999-12-31T23:59:59.99',
		  '9999-12-31T23:59:59.999',
		  '9999-12-31T23:59:59.9999',
		  '9999-12-31T23:59:59.99999',
		  '9999-12-31T23:59:59.999999',
		  '9999-12-31T23:59:59.9999999',
		  '9999-12-31T23:59:59.99999999'
		);`)

		rows2 := dbt.mustQuery("select * from t_ntz")
		defer rows2.Close()
		var c1, c2, c3, c4 time.Time
		for rows2.Next() {
			if err := rows2.Scan(&c1, &c2, &c3, &c4, &c5, &c6, &c7, &c8); err != nil {
				t.Errorf("values were not scanned: %v", err)
			}
		}

		expected = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
		if c8.Unix() != expected.Unix() || c8.Nanosecond() != nano {
			t.Errorf("the value did not match. expected: %v, got: %v", expected, c8)
		}
		if c7.Unix() != expected.Unix() || c7.Nanosecond() != nano-(nano%1e2) {
			t.Errorf("the value did not match. expected: %v, got: %v", expected, c7)
		}
		if c6.Unix() != expected.Unix() || c6.Nanosecond() != nano-(nano%1e3) {
			t.Errorf("the value did not match. expected: %v, got: %v", expected, c6)
		}
		if c5.Unix() != expected.Unix() || c5.Nanosecond() != nano-(nano%1e4) {
			t.Errorf("the value did not match. expected: %v, got: %v", expected, c5)
		}
		if c4.Unix() != expected.Unix() || c4.Nanosecond() != nano-(nano%1e5) {
			t.Errorf("the value did not match. expected: %v, got: %v", expected, c4)
		}
		if c3.Unix() != expected.Unix() || c3.Nanosecond() != nano-(nano%1e6) {
			t.Errorf("the value did not match. expected: %v, got: %v", expected, c3)
		}
		if c2.Unix() != expected.Unix() || c2.Nanosecond() != nano-(nano%1e7) {
			t.Errorf("the value did not match. expected: %v, got: %v", expected, c2)
		}
		if c1.Unix() != expected.Unix() || c1.Nanosecond() != nano-(nano%1e8) {
			t.Errorf("the value did not match. expected: %v, got: %v", expected, c1)
		}
	})
}

func TestArrowVariousTypes(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		rows := dbt.mustQueryContext(
			WithHigherPrecision(context.Background()), selectVariousTypes)
		defer rows.Close()
		if !rows.Next() {
			dbt.Error("failed to query")
		}
		cc, err := rows.Columns()
		if err != nil {
			dbt.Errorf("columns: %v", cc)
		}
		ct, err := rows.ColumnTypes()
		if err != nil {
			dbt.Errorf("column types: %v", ct)
		}
		var v1 *big.Float
		var v2, v2a int
		var v3 string
		var v4 float64
		var v5 []byte
		var v6 bool
		if err = rows.Scan(&v1, &v2, &v2a, &v3, &v4, &v5, &v6); err != nil {
			dbt.Errorf("failed to scan: %#v", err)
		}
		if v1.Cmp(big.NewFloat(1.0)) != 0 {
			dbt.Errorf("failed to scan. %#v", *v1)
		}
		if ct[0].Name() != "C1" || ct[1].Name() != "C2" || ct[2].Name() != "C2A" || ct[3].Name() != "C3" || ct[4].Name() != "C4" || ct[5].Name() != "C5" || ct[6].Name() != "C6" {
			dbt.Errorf("failed to get column names: %#v", ct)
		}
		if ct[0].ScanType() != reflect.TypeFor[*big.Float]() {
			dbt.Errorf("failed to get scan type. expected: %v, got: %v", reflect.TypeFor[float64](), ct[0].ScanType())
		}
		if ct[1].ScanType() != reflect.TypeFor[int64]() {
			dbt.Errorf("failed to get scan type. expected: %v, got: %v", reflect.TypeFor[int64](), ct[1].ScanType())
		}
		if ct[2].ScanType() != reflect.TypeFor[*big.Int]() {
			dbt.Errorf("failed to get scan type. expected: %v, got: %v", reflect.TypeFor[*big.Int](), ct[2].ScanType())
		}
		var pr, sc int64
		var cLen int64
		pr, sc = dbt.mustDecimalSize(ct[0])
		if pr != 30 || sc != 2 {
			dbt.Errorf("failed to get precision and scale. %#v", ct[0])
		}
		dbt.mustFailLength(ct[0])
		if canNull := dbt.mustNullable(ct[0]); canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[0])
		}
		if cLen != 0 {
			dbt.Errorf("failed to get length. %#v", ct[0])
		}
		if v2 != 2 {
			dbt.Errorf("failed to scan. %#v", v2)
		}
		pr, sc = dbt.mustDecimalSize(ct[1])
		if pr != 18 || sc != 0 {
			dbt.Errorf("failed to get precision and scale. %#v", ct[1])
		}
		dbt.mustFailLength(ct[1])
		if canNull := dbt.mustNullable(ct[1]); canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[1])
		}
		if v2a != 22 {
			dbt.Errorf("failed to scan. %#v", v2a)
		}
		dbt.mustFailLength(ct[2])
		if canNull := dbt.mustNullable(ct[2]); canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[2])
		}
		if v3 != "t3" {
			dbt.Errorf("failed to scan. %#v", v3)
		}
		dbt.mustFailDecimalSize(ct[3])
		if cLen = dbt.mustLength(ct[3]); cLen != 2 {
			dbt.Errorf("failed to get length. %#v", ct[3])
		}
		if canNull := dbt.mustNullable(ct[3]); canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[3])
		}
		if v4 != 4.2 {
			dbt.Errorf("failed to scan. %#v", v4)
		}
		dbt.mustFailDecimalSize(ct[4])
		dbt.mustFailLength(ct[4])
		if canNull := dbt.mustNullable(ct[4]); canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[4])
		}
		if !bytes.Equal(v5, []byte{0xab, 0xcd}) {
			dbt.Errorf("failed to scan. %#v", v5)
		}
		dbt.mustFailDecimalSize(ct[5])
		if cLen = dbt.mustLength(ct[5]); cLen != 8388608 { // BINARY
			dbt.Errorf("failed to get length. %#v", ct[5])
		}
		if canNull := dbt.mustNullable(ct[5]); canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[5])
		}
		if !v6 {
			dbt.Errorf("failed to scan. %#v", v6)
		}
		dbt.mustFailDecimalSize(ct[6])
		dbt.mustFailLength(ct[6])
	})
}

func TestArrowMemoryCleanedUp(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	runDBTest(t, func(dbt *DBTest) {
		ctx := WithArrowAllocator(
			context.Background(),
			mem,
		)

		rows := dbt.mustQueryContext(ctx, "select 1 UNION select 2 ORDER BY 1")
		defer rows.Close()
		var v int
		assertTrueF(t, rows.Next())
		assertNilF(t, rows.Scan(&v))
		assertEqualE(t, v, 1)
		assertTrueF(t, rows.Next())
		assertNilF(t, rows.Scan(&v))
		assertEqualE(t, v, 2)
		assertFalseE(t, rows.Next())
	})
}

// errReadCloser is a ReadCloser whose Close returns a predetermined error.
type errReadCloser struct {
	io.Reader
	closeErr error
	closed   bool
}

func (e *errReadCloser) Close() error {
	e.closed = true
	return e.closeErr
}

func TestArrowStreamBatchResetNilReader(t *testing.T) {
	// Reset on a batch with no reader should be a no-op and return nil.
	batch := ArrowStreamBatch{}
	err := batch.Reset()
	assertNilF(t, err, "Reset on nil reader should return nil")
}

func TestArrowStreamBatchResetClosesReader(t *testing.T) {
	// Reset should close the underlying reader and nil it out.
	rc := &errReadCloser{Reader: bytes.NewReader([]byte("data"))}
	batch := ArrowStreamBatch{rr: rc}

	err := batch.Reset()
	assertNilF(t, err, "Reset should not return error on successful close")
	assertTrueF(t, rc.closed, "underlying reader should have been closed")
	assertNilF(t, batch.rr, "rr should be nil after Reset")
}

func TestArrowStreamBatchResetPropagatesCloseError(t *testing.T) {
	// Reset should propagate the error from Close.
	expected := errors.New("close failed")
	rc := &errReadCloser{Reader: bytes.NewReader(nil), closeErr: expected}
	batch := ArrowStreamBatch{rr: rc}

	err := batch.Reset()
	assertTrueF(t, errors.Is(err, expected), "Reset should propagate close error")
	// rr should still be nilled out even when Close returns an error.
	assertNilF(t, batch.rr, "rr should be nil after Reset even on error")
}

func TestArrowStreamBatchResetAllowsRedownload(t *testing.T) {
	// After Reset, GetStream should re-invoke the download path.
	// We simulate this by setting rr, resetting, then confirming rr is nil
	// so GetStream would attempt a fresh download.
	rc := &errReadCloser{Reader: bytes.NewReader([]byte("stale"))}
	batch := ArrowStreamBatch{rr: rc}

	// Confirm GetStream returns the cached reader before Reset.
	stream, err := batch.GetStream(context.Background())
	assertNilF(t, err)
	assertTrueF(t, stream == rc, "GetStream should return cached reader")

	// Reset clears the cache.
	err = batch.Reset()
	assertNilF(t, err)
	assertNilF(t, batch.rr, "rr should be nil after Reset")
}

func TestArrowStreamBatchDoubleResetIsIdempotent(t *testing.T) {
	// Calling Reset twice should not error the second time.
	rc := &errReadCloser{Reader: bytes.NewReader([]byte("data"))}
	batch := ArrowStreamBatch{rr: rc}

	assertNilF(t, batch.Reset(), "first Reset should succeed")
	assertNilF(t, batch.Reset(), "second Reset should be a no-op")
}

// newTestArrowStreamBatch creates an ArrowStreamBatch wired to a mock FuncGet
// for unit testing without a live Snowflake connection.
func newTestArrowStreamBatch(funcGet func(context.Context, *snowflakeConn, string, map[string]string, time.Duration) (*http.Response, error)) ArrowStreamBatch {
	sc := &snowflakeConn{rest: &snowflakeRestful{RequestTimeout: 0}}
	scd := &snowflakeArrowStreamChunkDownloader{
		sc:         sc,
		ChunkMetas: []query.ExecResponseChunk{{URL: "http://fake/chunk0"}},
		Qrmk:       "testQrmk",
		FuncGet:    funcGet,
	}
	return ArrowStreamBatch{idx: 0, scd: scd}
}

func TestArrowStreamBatchResetThenGetStreamRedownloads(t *testing.T) {
	// After Reset, calling GetStream should invoke FuncGet again,
	// proving the chunk is actually re-downloaded.
	callCount := 0
	mockGet := func(_ context.Context, _ *snowflakeConn, _ string, _ map[string]string, _ time.Duration) (*http.Response, error) {
		callCount++
		body := []byte(fmt.Sprintf("payload-%d", callCount))
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	}

	batch := newTestArrowStreamBatch(mockGet)

	// First download.
	stream1, err := batch.GetStream(context.Background())
	assertNilF(t, err)
	data1, err := io.ReadAll(stream1)
	assertNilF(t, err)
	assertEqualE(t, callCount, 1)

	// Reset clears cached reader.
	assertNilF(t, batch.Reset())

	// Second download — FuncGet is called again.
	stream2, err := batch.GetStream(context.Background())
	assertNilF(t, err)
	data2, err := io.ReadAll(stream2)
	assertNilF(t, err)
	assertEqualE(t, callCount, 2)

	// Content should differ, proving it was re-downloaded.
	assertFalseE(t, bytes.Equal(data1, data2), "re-downloaded data should differ from original")
}

func TestArrowStreamBatchResetAfterPartialRead(t *testing.T) {
	// Simulates the primary use case: a mid-stream failure followed by
	// Reset + full re-download.
	fullPayload := []byte("complete-data-here")
	mockGet := func(_ context.Context, _ *snowflakeConn, _ string, _ map[string]string, _ time.Duration) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(fullPayload)),
		}, nil
	}

	batch := newTestArrowStreamBatch(mockGet)

	// Read only a few bytes (simulate partial/failed read).
	stream, err := batch.GetStream(context.Background())
	assertNilF(t, err)
	partial := make([]byte, 5)
	_, err = stream.Read(partial)
	assertNilF(t, err)

	// Reset and re-download the full payload.
	assertNilF(t, batch.Reset())
	stream2, err := batch.GetStream(context.Background())
	assertNilF(t, err)
	full, err := io.ReadAll(stream2)
	assertNilF(t, err)
	assertEqualE(t, string(full), string(fullPayload))
}

func TestArrowStreamBatchGetStreamGzipped(t *testing.T) {
	// Verify that gzip-compressed responses are transparently decompressed.
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write([]byte("decompressed payload"))
	assertNilF(t, err)
	assertNilF(t, gw.Close())
	gzBytes := buf.Bytes()

	mockGet := func(_ context.Context, _ *snowflakeConn, _ string, _ map[string]string, _ time.Duration) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(gzBytes)),
		}, nil
	}

	batch := newTestArrowStreamBatch(mockGet)
	stream, err := batch.GetStream(context.Background())
	assertNilF(t, err)
	data, err := io.ReadAll(stream)
	assertNilF(t, err)
	assertEqualE(t, string(data), "decompressed payload")
}

func TestArrowStreamBatchGetStreamNon200(t *testing.T) {
	// Non-200 responses should surface as a SnowflakeError.
	mockGet := func(_ context.Context, _ *snowflakeConn, _ string, _ map[string]string, _ time.Duration) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadGateway,
			Body:       io.NopCloser(bytes.NewReader([]byte("error"))),
		}, nil
	}

	batch := newTestArrowStreamBatch(mockGet)
	_, err := batch.GetStream(context.Background())
	var sfErr *SnowflakeError
	assertTrueF(t, errors.As(err, &sfErr), "should return SnowflakeError")
	assertEqualE(t, sfErr.Number, ErrFailedToGetChunk)
}

func TestArrowStreamBatchGetStreamFuncGetError(t *testing.T) {
	// Network-level errors from FuncGet should propagate directly.
	expected := errors.New("network failure")
	mockGet := func(_ context.Context, _ *snowflakeConn, _ string, _ map[string]string, _ time.Duration) (*http.Response, error) {
		return nil, expected
	}

	batch := newTestArrowStreamBatch(mockGet)
	_, err := batch.GetStream(context.Background())
	assertTrueF(t, errors.Is(err, expected), "should propagate FuncGet error")
}

func TestArrowStreamBatchResetThenGetStreamAfterError(t *testing.T) {
	// If the first GetStream fails, Reset should allow a successful retry.
	calls := 0
	mockGet := func(_ context.Context, _ *snowflakeConn, _ string, _ map[string]string, _ time.Duration) (*http.Response, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("transient error")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader([]byte("ok"))),
		}, nil
	}

	batch := newTestArrowStreamBatch(mockGet)

	// First attempt fails.
	_, err := batch.GetStream(context.Background())
	assertNotNilE(t, err)

	// Reset (rr is still nil since download failed, but Reset is safe).
	assertNilF(t, batch.Reset())

	// Retry succeeds.
	stream, err := batch.GetStream(context.Background())
	assertNilF(t, err)
	data, err := io.ReadAll(stream)
	assertNilF(t, err)
	assertEqualE(t, string(data), "ok")
}

func TestStreamWrapReaderCloseClosesInnerAndWrapped(t *testing.T) {
	// When inner Reader implements io.ReadCloser, both inner and wrapped
	// should be closed.
	inner := &errReadCloser{Reader: bytes.NewReader(nil)}
	wrappedClosed := false
	wrapped := &errReadCloser{
		Reader: bytes.NewReader(nil),
		closed: false,
	}
	w := &streamWrapReader{Reader: inner, wrapped: wrapped}
	err := w.Close()
	assertNilF(t, err)
	assertTrueF(t, inner.closed, "inner ReadCloser should be closed")
	assertTrueF(t, wrapped.closed, "wrapped body should be closed")
	_ = wrappedClosed
}

func TestStreamWrapReaderCloseNonClosableInner(t *testing.T) {
	// When inner Reader is not an io.ReadCloser, only wrapped is closed.
	wrapped := &errReadCloser{Reader: bytes.NewReader(nil)}
	w := &streamWrapReader{Reader: bytes.NewReader(nil), wrapped: wrapped}
	err := w.Close()
	assertNilF(t, err)
	assertTrueF(t, wrapped.closed, "wrapped body should be closed")
}

func TestStreamWrapReaderClosePropagatesInnerError(t *testing.T) {
	// If the inner ReadCloser's Close fails, the error should propagate.
	expected := errors.New("inner close fail")
	inner := &errReadCloser{Reader: bytes.NewReader(nil), closeErr: expected}
	wrapped := &errReadCloser{Reader: bytes.NewReader(nil)}
	w := &streamWrapReader{Reader: inner, wrapped: wrapped}
	err := w.Close()
	assertTrueF(t, errors.Is(err, expected), "should propagate inner close error")
	// wrapped should NOT have been closed because inner errored first.
	assertFalseE(t, wrapped.closed, "wrapped should not close if inner errors")
}

func TestArrowStreamBatchResetClosesStreamWrapReader(t *testing.T) {
	// Reset should properly close a streamWrapReader, including the
	// wrapped HTTP body.
	wrappedClosed := false
	body := &fakeResponseBody{
		body:    []byte("data"),
		onClose: func() { wrappedClosed = true },
	}
	batch := ArrowStreamBatch{
		rr: &streamWrapReader{
			Reader:  bytes.NewReader([]byte("inner")),
			wrapped: body,
		},
	}
	assertNilF(t, batch.Reset())
	assertNilF(t, batch.rr, "rr should be nil after Reset")
	assertTrueF(t, wrappedClosed, "wrapped HTTP body should be closed")
}

func TestArrowStreamBatchResetClosesGzipStreamWrapReader(t *testing.T) {
	// Verify Reset properly tears down a gzip + streamWrapReader stack.
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write([]byte("hello"))
	assertNilF(t, err)
	assertNilF(t, gw.Close())

	wrappedClosed := false
	body := &fakeResponseBody{
		body:    buf.Bytes(),
		onClose: func() { wrappedClosed = true },
	}
	gr, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	assertNilF(t, err)

	batch := ArrowStreamBatch{
		rr: &streamWrapReader{Reader: gr, wrapped: body},
	}
	assertNilF(t, batch.Reset())
	assertTrueF(t, wrappedClosed, "wrapped HTTP body should be closed via gzip reader")
}
