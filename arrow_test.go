// Copyright (c) 2020-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"database/sql/driver"
)

func TestArrowBatchHighPrecision(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		ctx := WithArrowBatches(context.Background())
		query := "select '0.1':: DECIMAL(38, 19) as c"

		var rows driver.Rows
		var err error

		// must use conn.Raw so we can get back driver rows (an interface)
		// which can be cast to snowflakeRows which exposes GetArrowBatch
		err = dbt.conn.Raw(func(x interface{}) error {
			queryer, implementsQueryContext := x.(driver.QueryerContext)
			assertTrueF(t, implementsQueryContext, "snowflake connection driver does not implement queryerContext")

			rows, err = queryer.QueryContext(WithArrowBatches(ctx), query, nil)
			return err
		})

		assertNilF(t, err, "error running select query")

		sfRows, isSfRows := rows.(SnowflakeRows)
		assertTrueF(t, isSfRows, "rows should be snowflakeRows")

		arrowBatches, err := sfRows.GetArrowBatches()
		assertNilF(t, err, "error getting arrow batches")
		assertNotEqualF(t, len(arrowBatches), 0, "should have at least one batch")

		c, err := arrowBatches[0].Fetch()
		assertNilF(t, err, "error fetching first batch")

		chunk := *c
		assertNotEqualF(t, len(chunk), 0, "should have at least one chunk")

		strVal := chunk[0].Column(0).ValueStr(0)
		expected := "0.1"
		assertEqualF(t, strVal, expected, fmt.Sprintf("should have returned 0.1, but got: %s", strVal))
	})
}

func TestArrowBigInt(t *testing.T) {
	conn := openConn(t)
	defer conn.Close()
	dbt := &DBTest{t, conn}

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
}

func TestArrowBigFloat(t *testing.T) {
	conn := openConn(t)
	defer conn.Close()
	dbt := &DBTest{t, conn}

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
}

func TestArrowIntPrecision(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	_, err := db.Exec(forceJSON)
	if err != nil {
		t.Fatalf("failed to set JSON as result type: %v", err)
	}

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
			rows, err := db.Query(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
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
			rows, err := db.Query(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
			defer rows.Close()
			if !rows.Next() {
				t.Error("failed to query")
			}
			defer rows.Close()
			var v int64
			if err := rows.Scan(&v); err == nil {
				t.Error("should fail to scan")
			}
		}
	})
	t.Run("arrow_enabled_scan_big_int", func(t *testing.T) {
		for _, tc := range intTestcases {
			rows, err := db.Query(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
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
			rows, err := db.QueryContext(WithHigherPrecision(context.Background()), fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
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
}

// TestArrowFloatPrecision tests the different variable types allowed in the
// rows.Scan() method. Note that for lower precision types we do not attempt
// to check the value as precision could be lost.
func TestArrowFloatPrecision(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	_, err := db.Exec(forceJSON)
	if err != nil {
		t.Fatalf("failed to set JSON as result type: %v", err)
	}

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
			rows, err := db.Query(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
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
			rows, err := db.Query(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
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
			rows, err := db.Query(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
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
	t.Run("arrow_enabled_scan_float64", func(t *testing.T) {
		for _, tc := range fltTestcases {
			rows, err := db.Query(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
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
			rows, err := db.Query(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
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
			rows, err := db.Query(fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
			defer rows.Close()
			if !rows.Next() {
				t.Error("failed to query")
			}
			var v string
			if err := rows.Scan(&v); err != nil {
				t.Errorf("failed to scan. %#v", err)
			}
		}
	})
	t.Run("arrow_high_precision_enabled_scan_big_float", func(t *testing.T) {
		for _, tc := range fltTestcases {
			rows, err := db.QueryContext(WithHigherPrecision(context.Background()), fmt.Sprintf(selectNumberSQL, tc.num, tc.prec, tc.sc))
			if err != nil {
				t.Fatalf("failed to query: %v", err)
			}
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
		var v2 int
		var v3 string
		var v4 float64
		var v5 []byte
		var v6 bool
		if err = rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6); err != nil {
			dbt.Errorf("failed to scan: %#v", err)
		}
		if v1.Cmp(big.NewFloat(1.0)) != 0 {
			dbt.Errorf("failed to scan. %#v", *v1)
		}
		if ct[0].Name() != "C1" || ct[1].Name() != "C2" || ct[2].Name() != "C3" || ct[3].Name() != "C4" || ct[4].Name() != "C5" || ct[5].Name() != "C6" {
			dbt.Errorf("failed to get column names: %#v", ct)
		}
		if ct[0].ScanType() != reflect.TypeOf(float64(0)) {
			dbt.Errorf("failed to get scan type. expected: %v, got: %v", reflect.TypeOf(float64(0)), ct[0].ScanType())
		}
		if ct[1].ScanType() != reflect.TypeOf(int64(0)) {
			dbt.Errorf("failed to get scan type. expected: %v, got: %v", reflect.TypeOf(int64(0)), ct[1].ScanType())
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
		if pr != 38 || sc != 0 {
			dbt.Errorf("failed to get precision and scale. %#v", ct[1])
		}
		dbt.mustFailLength(ct[1])
		if canNull := dbt.mustNullable(ct[1]); canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[1])
		}
		if v3 != "t3" {
			dbt.Errorf("failed to scan. %#v", v3)
		}
		dbt.mustFailDecimalSize(ct[2])
		if cLen = dbt.mustLength(ct[2]); cLen != 2 {
			dbt.Errorf("failed to get length. %#v", ct[2])
		}
		if canNull := dbt.mustNullable(ct[2]); canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[2])
		}
		if v4 != 4.2 {
			dbt.Errorf("failed to scan. %#v", v4)
		}
		dbt.mustFailDecimalSize(ct[3])
		dbt.mustFailLength(ct[3])
		if canNull := dbt.mustNullable(ct[3]); canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[3])
		}
		if !bytes.Equal(v5, []byte{0xab, 0xcd}) {
			dbt.Errorf("failed to scan. %#v", v5)
		}
		dbt.mustFailDecimalSize(ct[4])
		if cLen = dbt.mustLength(ct[4]); cLen != 8388608 { // BINARY
			dbt.Errorf("failed to get length. %#v", ct[4])
		}
		if canNull := dbt.mustNullable(ct[4]); canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[4])
		}
		if !v6 {
			dbt.Errorf("failed to scan. %#v", v6)
		}
		dbt.mustFailDecimalSize(ct[5])
		dbt.mustFailLength(ct[5])
		/*canNull = dbt.mustNullable(ct[5])
		if canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[5])
		}*/
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

		rows := dbt.mustQueryContext(ctx, "select 1 UNION select 2")
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
