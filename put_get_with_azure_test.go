// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPutFileWithAzure(t *testing.T) {
	if runningOnGithubAction() && !runningOnAzure() {
		t.Skip("skipping non azure environment")
	}
	testPutWithAzure(t, false)
}

func TestPutStreamWithAzure(t *testing.T) {
	if runningOnGithubAction() && !runningOnAzure() {
		t.Skip("skipping non azure environment")
	}
	testPutWithAzure(t, true)
}

func testPutWithAzure(t *testing.T, isStream bool) {
	tmpDir, _ := ioutil.TempDir("", "azure_put")
	defer os.RemoveAll(tmpDir)
	fname := filepath.Join(tmpDir, "test_put_get_with_azure.txt.gz")
	originalContents := "123,test1\n456,test2\n"
	tableName := randomString(5)

	var b bytes.Buffer
	gzw := gzip.NewWriter(&b)
	gzw.Write([]byte(originalContents))
	gzw.Close()
	if err := ioutil.WriteFile(fname, b.Bytes(), os.ModePerm); err != nil {
		t.Fatal("could not write to gzip file")
	}

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("create or replace table " + tableName + " (a int, b string)")
		fileStream, _ := os.OpenFile(fname, os.O_RDONLY, os.ModePerm)
		defer func() {
			dbt.mustExec("drop table " + tableName)
			if fileStream != nil {
				fileStream.Close()
			}
		}()

		var rows *RowsExtended
		sql := "put 'file://%v' @%%%v auto_compress=true parallel=30"
		if isStream {
			sqlText := fmt.Sprintf(sql, strings.ReplaceAll(fname, "\\", "\\\\"), tableName)
			rows = dbt.mustQueryContext(WithFileStream(context.Background(), fileStream), sqlText)
		} else {
			sqlText := fmt.Sprintf(sql, strings.ReplaceAll(fname, "\\", "\\\\"), tableName)
			rows = dbt.mustQuery(sqlText)
		}

		var s0, s1, s2, s3, s4, s5, s6, s7 string
		if rows.Next() {
			if err := rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7); err != nil {
				t.Fatal(err)
			}
		}
		if s6 != uploaded.String() {
			t.Fatalf("expected %v, got: %v", uploaded, s6)
		}

		dbt.mustExec("copy into " + tableName)
		dbt.mustExec("rm @%" + tableName)
		dbt.mustQueryAssertCount("ls @%"+tableName, 0)
	})
}
