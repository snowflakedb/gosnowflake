// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"reflect"
	"testing"
)

const (
	forceARROW = "ALTER SESSION SET GO_QUERY_RESULT_FORMAT = ARROW"
	forceJSON  = "ALTER SESSION SET GO_QUERY_RESULT_FORMAT = JSON"
)

func TestJSONInt(t *testing.T) {
	testInt(t, true)
}

func TestJSONFloat32(t *testing.T) {
	testFloat32(t, true)
}

func TestJSONFloat64(t *testing.T) {
	testFloat64(t, true)
}

func TestJSONVariousTypes(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec(forceJSON)
		rows := dbt.mustQuery(selectVariousTypes)
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
		var v1 float32
		var v2 int
		var v3 string
		var v4 float64
		var v5 []byte
		var v6 bool
		err = rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6)
		if err != nil {
			dbt.Errorf("failed to scan: %#v", err)
		}
		if v1 != 1.0 {
			dbt.Errorf("failed to scan. %#v", v1)
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
		var canNull bool
		pr, sc = dbt.mustDecimalSize(ct[0])
		if pr != 30 || sc != 2 {
			dbt.Errorf("failed to get precision and scale. %#v", ct[0])
		}
		dbt.mustFailLength(ct[0])
		canNull = dbt.mustNullable(ct[0])
		if canNull {
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
		canNull = dbt.mustNullable(ct[1])
		if canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[1])
		}
		if v3 != "t3" {
			dbt.Errorf("failed to scan. %#v", v3)
		}
		dbt.mustFailDecimalSize(ct[2])
		cLen = dbt.mustLength(ct[2])
		if cLen != 2 {
			dbt.Errorf("failed to get length. %#v", ct[2])
		}
		canNull = dbt.mustNullable(ct[2])
		if canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[2])
		}
		if v4 != 4.2 {
			dbt.Errorf("failed to scan. %#v", v4)
		}
		dbt.mustFailDecimalSize(ct[3])
		dbt.mustFailLength(ct[3])
		canNull = dbt.mustNullable(ct[3])
		if canNull {
			dbt.Errorf("failed to get nullable. %#v", ct[3])
		}
		if !bytes.Equal(v5, []byte{0xab, 0xcd}) {
			dbt.Errorf("failed to scan. %#v", v5)
		}
		dbt.mustFailDecimalSize(ct[4])
		cLen = dbt.mustLength(ct[4]) // BINARY
		if cLen != 8388608 {
			dbt.Errorf("failed to get length. %#v", ct[4])
		}
		canNull = dbt.mustNullable(ct[4])
		if canNull {
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

func TestJSONString(t *testing.T) {
	testString(t, true)
}

func TestJSONSimpleDateTimeTimestampFetch(t *testing.T) {
	testSimpleDateTimeTimestampFetch(t, true)
}

func TestJSONDateTime(t *testing.T) {
	testDateTime(t, true)
}

func TestJSONTimestampLTZ(t *testing.T) {
	testTimestampLTZ(t, true)
}

func TestJSONTimestampTZ(t *testing.T) {
	testTimestampTZ(t, true)
}

func TestJSONNULL(t *testing.T) {
	testNULL(t, true)
}

func TestJSONVariant(t *testing.T) {
	testVariant(t, true)
}

func TestJSONArray(t *testing.T) {
	testArray(t, true)
}

func TestLargeSetJSONResultWithDecoder(t *testing.T) {
	testLargeSetResult(t, 10000, true)
}

func TestLargeSetResultWithCustomJSONDecoder(t *testing.T) {
	CustomJSONDecoderEnabled = true
	// less number of rows to avoid Travis timeout
	testLargeSetResult(t, 20000, true)
}

func TestBindingJSONInterface(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec(forceJSON)
		rows := dbt.mustQuery(selectVariousTypes)
		defer rows.Close()
		if !rows.Next() {
			dbt.Error("failed to query")
		}
		var v1, v2, v3, v4, v5, v6 interface{}
		if err := rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6); err != nil {
			dbt.Errorf("failed to scan: %#v", err)
		}
		if s, ok := v1.(string); !ok || s != "1.00" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v1)
		}
		if s, ok := v2.(string); !ok || s != "2" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v2)
		}
		if s, ok := v3.(string); !ok || s != "t3" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v3)
		}
		if s, ok := v4.(string); !ok || s != "4.2" {
			dbt.Fatalf("failed to fetch. ok: %v, value: %v", ok, v4)
		}
	})
}
