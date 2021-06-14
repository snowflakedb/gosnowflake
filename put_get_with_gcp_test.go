// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPutWithGCP(t *testing.T) {
	if runningOnGithubAction() && !runningOnGCP() {
		t.Skip("skipping non gcp environment")
	}
	tmpDir, _ := ioutil.TempDir("", "gcp_put")
	defer os.RemoveAll(tmpDir)
	fname := filepath.Join(tmpDir, "test_put_get_with_gcp.txt.gz")
	originalContents := "123,test1\n456,test2\n"

	var b bytes.Buffer
	gzw := gzip.NewWriter(&b)
	gzw.Write([]byte(originalContents))
	gzw.Close()
	err := ioutil.WriteFile(fname, b.Bytes(), os.ModePerm)
	if err != nil {
		t.Fatal("could not write to gzip file")
	}
	tableName := randomString(5)

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("create or replace table " + tableName + " (a int, b string)")
		defer dbt.mustExec("drop table " + tableName)
		sql := "put 'file://%v' @%%%v auto_compress=true parallel=30"
		sqlText := fmt.Sprintf(sql, strings.ReplaceAll(fname, "\\", "\\\\"),
			tableName)
		rows := dbt.mustQuery(sqlText)
		var s0, s1, s2, s3, s4, s5, s6, s7 string
		if rows.Next() {
			if err = rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7); err != nil {
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
