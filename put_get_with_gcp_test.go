// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestPutGetWithGCP(t *testing.T) {
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

		dbt.mustExec(fmt.Sprintf("copy into @%%%v from %v file_format=("+
			"type=csv compression='gzip')", tableName, tableName))

		rows = dbt.mustQuery(fmt.Sprintf("get @%%%v 'file://%v'", tableName, tmpDir))
		defer rows.Close()
		for rows.Next() {
			if err := rows.Scan(&s0, &s1, &s2, &s3); err != nil {
				t.Error(err)
			}
			if !strings.HasPrefix(s0, "data_") {
				t.Error("a file was not downloaded by GET")
			}
			if v, err := strconv.Atoi(s1); err != nil || v != 36 {
				t.Error("did not return the right file size")
			}
			if s2 != "DOWNLOADED" {
				t.Error("did not return DOWNLOADED status")
			}
			if s3 != "" {
				t.Errorf("returned %v", s3)
			}
		}

		files, err := filepath.Glob(filepath.Join(tmpDir, "data_*"))
		if err != nil {
			t.Error(err)
		}
		fileName := files[0]
		f, _ := os.Open(fileName)
		gz, err := gzip.NewReader(f)
		if err != nil {
			t.Error(err)
		}
		var contents string
		for {
			c := make([]byte, defaultChunkBufferSize)
			if n, err := gz.Read(c); err != nil {
				if err == io.EOF {
					contents = contents + string(c[:n])
					break
				}
				t.Error(err)
			} else {
				contents = contents + string(c[:n])
			}
		}

		if contents != originalContents {
			t.Error("output is different from the original file")
		}
	})
}
