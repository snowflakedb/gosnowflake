// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const createStageStmt = "CREATE OR REPLACE STAGE %v URL = '%v' CREDENTIALS = (%v)"

func randomString(n int) string {
	rand.Seed(time.Now().UnixNano())
	alpha := []rune("abcdefghijklmnopqrstuvwxyz")
	b := make([]rune, n)
	for i := range b {
		b[i] = alpha[rand.Intn(len(alpha))]
	}
	return string(b)
}

func TestPutError(t *testing.T) {
	if isWindows {
		t.Skip("permission model is different")
	}
	tmpDir, err := os.MkdirTemp("", "putfiledir")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)
	file1 := filepath.Join(tmpDir, "file1")
	remoteLocation := filepath.Join(tmpDir, "remote_loc")
	f, err := os.OpenFile(file1, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Error(err)
	}
	f.WriteString("test1")
	os.Chmod(file1, 0000)

	data := &execResponseData{
		Command:           string(uploadCommand),
		AutoCompress:      false,
		SrcLocations:      []string{file1},
		SourceCompression: "none",
		StageInfo: execResponseStageInfo{
			Location:     remoteLocation,
			LocationType: string(local),
			Path:         "remote_loc",
		},
	}

	fta := &snowflakeFileTransferAgent{
		data: data,
		options: &SnowflakeFileTransferOptions{
			RaisePutGetError: false,
		},
	}
	if err = fta.execute(); err != nil {
		t.Fatal(err)
	}
	if _, err = fta.result(); err != nil {
		t.Fatal(err)
	}

	fta = &snowflakeFileTransferAgent{
		data: data,
		options: &SnowflakeFileTransferOptions{
			RaisePutGetError: true,
		},
	}
	if err = fta.execute(); err != nil {
		t.Fatal(err)
	}
	if _, err = fta.result(); err == nil {
		t.Fatalf("should raise permission error")
	}
}

func TestPercentage(t *testing.T) {
	testcases := []struct {
		seen     int64
		size     float64
		expected float64
	}{
		{0, 0, 1.0},
		{20, 0, 1.0},
		{40, 20, 1.0},
		{14, 28, 0.5},
	}
	for _, test := range testcases {
		if percent(test.seen, test.size) != test.expected {
			t.Fatalf("percentage conversion failed. %v/%v, expected: %v, got: %v",
				test.seen, test.size, test.expected, percent(test.seen, test.size))
		}
	}
}

type tcPutGetData struct {
	dir                string
	awsAccessKeyID     string
	awsSecretAccessKey string
	stage              string
	warehouse          string
	database           string
	userBucket         string
}

func cleanupPut(dbt *DBTest, td *tcPutGetData) {
	dbt.mustExec("drop database " + td.database)
	dbt.mustExec("drop warehouse " + td.warehouse)
}

func getAWSCredentials() (string, string, string, error) {
	keyID, ok := os.LookupEnv("AWS_ACCESS_KEY_ID")
	if !ok {
		return "", "", "", fmt.Errorf("key id invalid")
	}
	secretKey, ok := os.LookupEnv("AWS_SECRET_ACCESS_KEY")
	if !ok {
		return keyID, "", "", fmt.Errorf("secret key invalid")
	}
	bucket, present := os.LookupEnv("SF_AWS_USER_BUCKET")
	if !present {
		user, err := user.Current()
		if err != nil {
			return keyID, secretKey, "", err
		}
		bucket = fmt.Sprintf("sfc-dev1-regression/%v/reg", user.Username)
	}
	return keyID, secretKey, bucket, nil
}

func createTestData(dbt *DBTest) (*tcPutGetData, error) {
	keyID, secretKey, bucket, err := getAWSCredentials()
	if err != nil {
		return nil, err
	}
	uniqueName := randomString(10)
	database := fmt.Sprintf("%v_db", uniqueName)
	wh := fmt.Sprintf("%v_wh", uniqueName)

	dir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	ret := tcPutGetData{
		dir,
		keyID,
		secretKey,
		fmt.Sprintf("%v_stage", uniqueName),
		wh,
		database,
		bucket,
	}

	if _, err = dbt.db.Exec("use role sysadmin"); err != nil {
		return nil, err
	}
	dbt.mustExec(fmt.Sprintf(
		"create or replace warehouse %v warehouse_size='small' "+
			"warehouse_type='standard' auto_suspend=1800", wh))
	dbt.mustExec("create or replace database " + database)
	dbt.mustExec("create or replace schema gotesting_schema")
	dbt.mustExec("create or replace file format VSV type = 'CSV' " +
		"field_delimiter='|' error_on_column_count_mismatch=false")
	return &ret, nil
}

func TestPutLocalFile(t *testing.T) {
	if runningOnGithubAction() && !runningOnAWS() {
		t.Skip("skipping non aws environment")
	}
	runTests(t, dsn, func(dbt *DBTest) {
		data, err := createTestData(dbt)
		if err != nil {
			t.Skip("snowflake admin account not accessible")
		}
		defer cleanupPut(dbt, data)
		dbt.mustExec("use warehouse " + data.warehouse)
		dbt.mustExec("alter session set DISABLE_PUT_AND_GET_ON_EXTERNAL_STAGE=false")
		dbt.mustExec("use schema " + data.database + ".gotesting_schema")
		execQuery := fmt.Sprintf(
			`create or replace table gotest_putget_t1 (c1 STRING, c2 STRING,
			c3 STRING, c4 STRING, c5 STRING, c6 STRING, c7 STRING, c8 STRING,
			c9 STRING) stage_file_format = ( field_delimiter = '|'
			error_on_column_count_mismatch=false) stage_copy_options =
			(purge=false) stage_location = (url = 's3://%v/%v' credentials =
			(AWS_KEY_ID='%v' AWS_SECRET_KEY='%v'))`,
			data.userBucket,
			data.stage,
			data.awsAccessKeyID,
			data.awsSecretAccessKey)
		dbt.mustExec(execQuery)
		defer dbt.mustExec("drop table if exists gotest_putget_t1")

		execQuery = fmt.Sprintf(`put file://%v/test_data/orders_10*.csv
			@%%gotest_putget_t1`, data.dir)
		dbt.mustExec(execQuery)
		dbt.mustQueryAssertCount("ls @%gotest_putget_t1", 2)

		var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9 string
		rows := dbt.mustQuery("copy into gotest_putget_t1")
		for rows.Next() {
			rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9)
			if s1 != "LOADED" {
				t.Fatal("not loaded")
			}
		}

		rows = dbt.mustQuery("select count(*) from gotest_putget_t1")
		var i int
		if rows.Next() {
			rows.Scan(&i)
			if i != 75 {
				t.Fatalf("expected 75 rows, got %v", i)
			}
		}

		rows = dbt.mustQuery(`select STATUS from information_schema
			.load_history where table_name='gotest_putget_t1'`)
		if rows.Next() {
			rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9)
			if s1 != "LOADED" {
				t.Fatal("not loaded")
			}
		}
	})
}

func TestPutWithAutoCompressFalse(t *testing.T) {
	if runningOnGithubAction() && !runningOnAWS() {
		t.Skip("skipping non aws environment")
	}
	tmpDir, err := os.MkdirTemp("", "put")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)
	testData := filepath.Join(tmpDir, "data.txt")
	f, err := os.OpenFile(testData, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Error(err)
	}
	f.WriteString("test1,test2\ntest3,test4")
	f.Sync()
	defer f.Close()

	runTests(t, dsn, func(dbt *DBTest) {
		if _, err = dbt.db.Exec("use role sysadmin"); err != nil {
			t.Skip("snowflake admin account not accessible")
		}
		dbt.mustExec("rm @~/test_put_uncompress_file")
		sqlText := fmt.Sprintf("put file://%v @~/test_put_uncompress_file auto_compress=FALSE", testData)
		sqlText = strings.ReplaceAll(sqlText, "\\", "\\\\")
		dbt.mustExec(sqlText)
		defer dbt.mustExec("rm @~/test_put_uncompress_file")
		rows := dbt.mustQuery("ls @~/test_put_uncompress_file")
		var file, s1, s2, s3 string
		if rows.Next() {
			if err := rows.Scan(&file, &s1, &s2, &s3); err != nil {
				t.Fatal(err)
			}
		}
		if !strings.Contains(file, "test_put_uncompress_file/data.txt") {
			t.Fatalf("should contain file. got: %v", file)
		}
		if strings.Contains(file, "data.txt.gz") {
			t.Fatalf("should not contain file. got: %v", file)
		}
	})
}

func TestPutOverwrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "data")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)
	testData := filepath.Join(tmpDir, "data.txt")
	f, err := os.OpenFile(testData, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		t.Error(err)
	}
	f.WriteString("test1,test2\ntest3,test4\n")
	f.Close()

	runTests(t, dsn, func(dbt *DBTest) {
		if _, err = dbt.db.Exec("use role sysadmin"); err != nil {
			t.Skip("snowflake admin account not accessible")
		}
		dbt.mustExec("rm @~/test_put_overwrite")

		f, _ = os.Open(testData)
		rows := dbt.mustQueryContext(
			WithFileStream(context.Background(), f),
			fmt.Sprintf("put 'file://%v' @~/test_put_overwrite",
				strings.ReplaceAll(testData, "\\", "\\\\")))
		f.Close()
		defer dbt.mustExec("rm @~/test_put_overwrite")
		var s0, s1, s2, s3, s4, s5, s6, s7 string
		if rows.Next() {
			if err = rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7); err != nil {
				t.Fatal(err)
			}
		}
		if s6 != uploaded.String() {
			t.Fatalf("expected UPLOADED, got %v", s6)
		}

		f, _ = os.Open(testData)
		ctx := WithFileTransferOptions(context.Background(),
			&SnowflakeFileTransferOptions{
				DisablePutOverwrite: true,
			})
		rows = dbt.mustQueryContext(
			WithFileStream(ctx, f),
			fmt.Sprintf("put 'file://%v' @~/test_put_overwrite",
				strings.ReplaceAll(testData, "\\", "\\\\")))
		f.Close()
		if rows.Next() {
			if err = rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7); err != nil {
				t.Fatal(err)
			}
		}
		if s6 != skipped.String() {
			t.Fatalf("expected SKIPPED, got %v", s6)
		}

		f, _ = os.Open(testData)
		rows = dbt.mustQueryContext(
			WithFileStream(context.Background(), f),
			fmt.Sprintf("put 'file://%v' @~/test_put_overwrite overwrite=true",
				strings.ReplaceAll(testData, "\\", "\\\\")))
		f.Close()
		if rows.Next() {
			if err = rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7); err != nil {
				t.Fatal(err)
			}
		}
		if s6 != uploaded.String() {
			t.Fatalf("expected UPLOADED, got %v", s6)
		}

		rows = dbt.mustQuery("ls @~/test_put_overwrite")
		if rows.Next() {
			if err = rows.Scan(&s0, &s1, &s2, &s3); err != nil {
				t.Fatal(err)
			}
		}
		if s0 != fmt.Sprintf("test_put_overwrite/%v.gz", baseName(testData)) {
			t.Fatalf("expected test_put_overwrite/%v.gz, got %v", baseName(testData), s0)
		}
	})
}

func TestPutGetFile(t *testing.T) {
	testPutGet(t, false)
}

func TestPutGetStream(t *testing.T) {
	testPutGet(t, true)
}

func testPutGet(t *testing.T, isStream bool) {
	tmpDir, err := os.MkdirTemp("", "put_get")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)
	fname := filepath.Join(tmpDir, "test_put_get.txt.gz")
	originalContents := "123,test1\n456,test2\n"
	tableName := randomString(5)

	var b bytes.Buffer
	gzw := gzip.NewWriter(&b)
	gzw.Write([]byte(originalContents))
	gzw.Close()
	if err = os.WriteFile(fname, b.Bytes(), os.ModePerm); err != nil {
		t.Fatal("could not write to gzip file")
	}

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("create or replace table " + tableName +
			" (a int, b string)")
		fileStream, err := os.OpenFile(fname, os.O_RDONLY, os.ModePerm)
		if err != nil {
			t.Error(err)
		}
		defer func() {
			defer dbt.mustExec("drop table " + tableName)
			if fileStream != nil {
				fileStream.Close()
			}
		}()

		var sqlText string
		var rows *RowsExtended
		sql := "put 'file://%v' @%%%v auto_compress=true parallel=30"
		ctx := context.Background()
		if isStream {
			sqlText = fmt.Sprintf(
				sql, strings.ReplaceAll(fname, "\\", "\\\\"), tableName)
			rows = dbt.mustQueryContext(WithFileStream(ctx, fileStream), sqlText)
		} else {
			sqlText = fmt.Sprintf(
				sql, strings.ReplaceAll(fname, "\\", "\\\\"), tableName)
			rows = dbt.mustQuery(sqlText)
		}

		var s0, s1, s2, s3, s4, s5, s6, s7 string
		if rows.Next() {
			if err = rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7); err != nil {
				t.Fatal(err)
			}
		}
		if s6 != uploaded.String() {
			t.Fatalf("expected %v, got: %v", uploaded, s6)
		}
		// check file is PUT
		dbt.mustQueryAssertCount("ls @%"+tableName, 1)

		dbt.mustExec("copy into " + tableName)
		dbt.mustExec("rm @%" + tableName)
		dbt.mustQueryAssertCount("ls @%"+tableName, 0)

		dbt.mustExec(fmt.Sprintf(`copy into @%%%v from %v file_format=(type=csv
			compression='gzip')`, tableName, tableName))

		sql = fmt.Sprintf("get @%%%v 'file://%v'", tableName, tmpDir)
		sqlText = strings.ReplaceAll(sql, "\\", "\\\\")
		rows = dbt.mustQuery(sqlText)
		defer rows.Close()
		for rows.Next() {
			if err = rows.Scan(&s0, &s1, &s2, &s3); err != nil {
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
			t.Fatal(err)
		}
		fileName := files[0]
		f, err := os.Open(fileName)
		if err != nil {
			t.Error(err)
		}
		defer f.Close()
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
func TestPutGetGcsDownscopedCredential(t *testing.T) {
	if runningOnGithubAction() && !runningOnGCP() {
		t.Skip("skipping non GCP environment")
	}

	tmpDir, err := os.MkdirTemp("", "put_get")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)
	fname := filepath.Join(tmpDir, "test_put_get.txt.gz")
	originalContents := "123,test1\n456,test2\n"
	tableName := randomString(5)

	var b bytes.Buffer
	gzw := gzip.NewWriter(&b)
	gzw.Write([]byte(originalContents))
	gzw.Close()
	if err = os.WriteFile(fname, b.Bytes(), os.ModePerm); err != nil {
		t.Fatal("could not write to gzip file")
	}

	dsn = dsn + "&GCS_USE_DOWNSCOPED_CREDENTIAL=true"
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("create or replace table " + tableName +
			" (a int, b string)")
		fileStream, err := os.OpenFile(fname, os.O_RDONLY, os.ModePerm)
		if err != nil {
			t.Error(err)
		}
		defer func() {
			defer dbt.mustExec("drop table " + tableName)
			if fileStream != nil {
				fileStream.Close()
			}
		}()

		var sqlText string
		var rows *RowsExtended
		sql := "put 'file://%v' @%%%v auto_compress=true parallel=30"
		sqlText = fmt.Sprintf(
			sql, strings.ReplaceAll(fname, "\\", "\\\\"), tableName)
		rows = dbt.mustQuery(sqlText)

		var s0, s1, s2, s3, s4, s5, s6, s7 string
		if rows.Next() {
			if err = rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7); err != nil {
				t.Fatal(err)
			}
		}
		if s6 != uploaded.String() {
			t.Fatalf("expected %v, got: %v", uploaded, s6)
		}
		// check file is PUT
		dbt.mustQueryAssertCount("ls @%"+tableName, 1)

		dbt.mustExec("copy into " + tableName)
		dbt.mustExec("rm @%" + tableName)
		dbt.mustQueryAssertCount("ls @%"+tableName, 0)

		dbt.mustExec(fmt.Sprintf(`copy into @%%%v from %v file_format=(type=csv
            compression='gzip')`, tableName, tableName))

		sql = fmt.Sprintf("get @%%%v 'file://%v'", tableName, tmpDir)
		sqlText = strings.ReplaceAll(sql, "\\", "\\\\")
		rows = dbt.mustQuery(sqlText)
		defer rows.Close()
		for rows.Next() {
			if err = rows.Scan(&s0, &s1, &s2, &s3); err != nil {
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
			t.Fatal(err)
		}
		fileName := files[0]
		f, err := os.Open(fileName)
		if err != nil {
			t.Error(err)
		}
		defer f.Close()
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
