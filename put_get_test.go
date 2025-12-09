package gosnowflake

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const createStageStmt = "CREATE OR REPLACE STAGE %v URL = '%v' CREDENTIALS = (%v)"

func TestPutError(t *testing.T) {
	if isWindows {
		t.Skip("permission model is different")
	}
	tmpDir := t.TempDir()
	file1 := filepath.Join(tmpDir, "file1")
	remoteLocation := filepath.Join(tmpDir, "remote_loc")
	f, err := os.Create(file1)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		assertNilF(t, f.Close())
	}()
	_, err = f.WriteString("test1")
	assertNilF(t, err)
	assertNilF(t, os.Chmod(file1, 0000))
	defer func() {
		assertNilF(t, os.Chmod(file1, 0644))
	}()

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
		ctx:  context.Background(),
		data: data,
		options: &SnowflakeFileTransferOptions{
			RaisePutGetError: false,
		},
		sc: &snowflakeConn{
			cfg: &Config{},
		},
	}
	if err = fta.execute(); err != nil {
		t.Fatal(err)
	}
	if _, err = fta.result(); err != nil {
		t.Fatal(err)
	}

	fta = &snowflakeFileTransferAgent{
		ctx:  context.Background(),
		data: data,
		options: &SnowflakeFileTransferOptions{
			RaisePutGetError: true,
		},
		sc: &snowflakeConn{
			cfg: &Config{},
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
		t.Run(fmt.Sprintf("%v_%v_%v", test.seen, test.size, test.expected), func(t *testing.T) {
			spp := snowflakeProgressPercentage{}
			if spp.percent(test.seen, test.size) != test.expected {
				t.Fatalf("percentage conversion failed. %v/%v, expected: %v, got: %v",
					test.seen, test.size, test.expected, spp.percent(test.seen, test.size))
			}
		})
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
		bucket = fmt.Sprintf("sfc-eng-regression/%v/reg", user.Username)
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

	if _, err = dbt.exec("use role sysadmin"); err != nil {
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
	runDBTest(t, func(dbt *DBTest) {
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

		var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9 sql.NullString
		rows := dbt.mustQuery("copy into gotest_putget_t1")
		defer func() {
			assertNilF(t, rows.Close())
		}()
		for rows.Next() {
			assertNilF(t, rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9))
			if !s1.Valid || s1.String != "LOADED" {
				t.Fatal("not loaded")
			}
		}

		rows2 := dbt.mustQuery("select count(*) from gotest_putget_t1")
		defer func() {
			assertNilF(t, rows2.Close())
		}()
		var i int
		if rows2.Next() {
			assertNilF(t, rows2.Scan(&i))
			if i != 75 {
				t.Fatalf("expected 75 rows, got %v", i)
			}
		}

		rows3 := dbt.mustQuery(`select STATUS from information_schema .load_history where table_name='gotest_putget_t1'`)
		defer func() {
			assertNilF(t, rows3.Close())
		}()
		if rows3.Next() {
			assertNilF(t, rows3.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9))
			if !s1.Valid || s1.String != "LOADED" {
				t.Fatal("not loaded")
			}
		}
	})
}

func TestPutGetWithAutoCompressFalse(t *testing.T) {
	tmpDir := t.TempDir()
	testData := filepath.Join(tmpDir, "data.txt")
	f, err := os.Create(testData)
	if err != nil {
		t.Error(err)
	}
	originalContents := "test1,test2\ntest3,test4"
	_, err = f.WriteString(originalContents)
	assertNilF(t, err)
	assertNilF(t, f.Sync())
	defer func() {
		assertNilF(t, f.Close())
	}()

	runDBTest(t, func(dbt *DBTest) {
		stageDir := "test_put_uncompress_file_" + randomString(10)
		dbt.mustExec("rm @~/" + stageDir)

		// PUT test
		sqlText := fmt.Sprintf("put 'file://%v' @~/%v auto_compress=FALSE", testData, stageDir)
		sqlText = strings.ReplaceAll(sqlText, "\\", "\\\\")
		dbt.mustExec(sqlText)
		defer dbt.mustExec("rm @~/" + stageDir)
		rows := dbt.mustQuery("ls @~/" + stageDir)
		defer func() {
			assertNilF(t, rows.Close())
		}()
		var file, s1, s2, s3 string
		if rows.Next() {
			err = rows.Scan(&file, &s1, &s2, &s3)
			assertNilE(t, err)
		}
		assertTrueF(t, strings.Contains(file, stageDir+"/data.txt"), fmt.Sprintf("should contain file. got: %v", file))
		assertFalseF(t, strings.Contains(file, "data.txt.gz"), fmt.Sprintf("should not contain file. got: %v", file))

		// GET test
		var streamBuf bytes.Buffer
		ctx := WithFileTransferOptions(context.Background(), &SnowflakeFileTransferOptions{GetFileToStream: true})
		ctx = WithFileGetStream(ctx, &streamBuf)
		sql := fmt.Sprintf("get @~/%v/data.txt 'file://%v'", stageDir, tmpDir)
		sqlText = strings.ReplaceAll(sql, "\\", "\\\\")
		rows2 := dbt.mustQueryContext(ctx, sqlText)
		defer func() {
			assertNilF(t, rows2.Close())
		}()
		for rows2.Next() {
			err = rows2.Scan(&file, &s1, &s2, &s3)
			assertNilE(t, err)
			assertTrueE(t, strings.HasPrefix(file, "data.txt"), "a file was not downloaded by GET")
			v, err := strconv.Atoi(s1)
			assertNilE(t, err)
			assertEqualE(t, v, 23, "did not return the right file size")
			assertEqualE(t, s2, "DOWNLOADED", "did not return DOWNLOADED status")
			assertEqualE(t, s3, "")
		}
		var contents string
		r := bytes.NewReader(streamBuf.Bytes())
		for {
			c := make([]byte, defaultChunkBufferSize)
			if n, err := r.Read(c); err != nil {
				if err == io.EOF {
					contents = contents + string(c[:n])
					break
				}
				t.Error(err)
			} else {
				contents = contents + string(c[:n])
			}
		}
		assertEqualE(t, contents, originalContents)
	})
}

func TestPutOverwrite(t *testing.T) {
	tmpDir := t.TempDir()
	testData := filepath.Join(tmpDir, "data.txt")
	f, err := os.Create(testData)
	if err != nil {
		t.Error(err)
	}
	_, err = f.WriteString("test1,test2\ntest3,test4\n")
	assertNilF(t, err)
	assertNilF(t, f.Close())

	stageName := "test_put_overwrite_stage_" + randomString(10)

	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("CREATE OR REPLACE STAGE " + stageName)
		defer dbt.mustExec("DROP STAGE " + stageName)

		f, _ = os.Open(testData)
		rows := dbt.mustQueryContext(
			WithFileStream(context.Background(), f),
			fmt.Sprintf("put 'file://%v' @"+stageName+"/test_put_overwrite",
				strings.ReplaceAll(testData, "\\", "/")))
		defer rows.Close()
		f.Close()
		var s0, s1, s2, s3, s4, s5, s6, s7 string
		if rows.Next() {
			if err = rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7); err != nil {
				t.Fatal(err)
			}
		}
		if s6 != uploaded.String() {
			t.Fatalf("expected UPLOADED, got %v", s6)
		}

		rows = dbt.mustQuery("ls @" + stageName + "/test_put_overwrite")
		defer rows.Close()
		assertTrueF(t, rows.Next(), "expected new rows")
		if err = rows.Scan(&s0, &s1, &s2, &s3); err != nil {
			t.Fatal(err)
		}
		md5Column := s2

		f, _ = os.Open(testData)
		rows = dbt.mustQueryContext(
			WithFileStream(context.Background(), f),
			fmt.Sprintf("put 'file://%v' @"+stageName+"/test_put_overwrite",
				strings.ReplaceAll(testData, "\\", "/")))
		defer rows.Close()
		f.Close()
		assertTrueF(t, rows.Next(), "expected new rows")
		if err = rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7); err != nil {
			t.Fatal(err)
		}
		if s6 != skipped.String() {
			t.Fatalf("expected SKIPPED, got %v", s6)
		}

		rows = dbt.mustQuery("ls @" + stageName + "/test_put_overwrite")
		defer rows.Close()
		assertTrueF(t, rows.Next(), "expected new rows")

		if err = rows.Scan(&s0, &s1, &s2, &s3); err != nil {
			t.Fatal(err)
		}
		if s2 != md5Column {
			t.Fatal("The MD5 column should have stayed the same")
		}

		f, _ = os.Open(testData)
		rows = dbt.mustQueryContext(
			WithFileStream(context.Background(), f),
			fmt.Sprintf("put 'file://%v' @"+stageName+"/test_put_overwrite overwrite=true",
				strings.ReplaceAll(testData, "\\", "/")))
		defer rows.Close()
		f.Close()
		assertTrueF(t, rows.Next(), "expected new rows")
		if err = rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7); err != nil {
			t.Fatal(err)
		}
		if s6 != uploaded.String() {
			t.Fatalf("expected UPLOADED, got %v", s6)
		}

		rows = dbt.mustQuery("ls @" + stageName + "/test_put_overwrite")
		defer rows.Close()
		assertTrueF(t, rows.Next(), "expected new rows")
		if err = rows.Scan(&s0, &s1, &s2, &s3); err != nil {
			t.Fatal(err)
		}
		assertEqualE(t, s0, stageName+"/test_put_overwrite/"+baseName(testData)+".gz")
		assertNotEqualE(t, s2, md5Column)
	})
}

func TestPutGetFile(t *testing.T) {
	testPutGet(t, false)
}

func TestPutGetStream(t *testing.T) {
	testPutGet(t, true)
}

func testPutGet(t *testing.T, isStream bool) {
	tmpDir := t.TempDir()
	fname := filepath.Join(tmpDir, "test_put_get.txt.gz")
	originalContents := "123,test1\n456,test2\n"
	tableName := randomString(5)

	var b bytes.Buffer
	gzw := gzip.NewWriter(&b)
	_, err := gzw.Write([]byte(originalContents))
	assertNilF(t, err)
	assertNilF(t, gzw.Close())
	if err := os.WriteFile(fname, b.Bytes(), readWriteFileMode); err != nil {
		t.Fatal("could not write to gzip file")
	}

	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("create or replace table " + tableName +
			" (a int, b string)")
		defer dbt.mustExec("drop table " + tableName)
		fileStream, err := os.Open(fname)
		assertNilF(t, err)
		defer func() {
			assertNilF(t, fileStream.Close())
		}()

		var sqlText string
		var rows *RowsExtended
		sql := "put 'file://%v' @%%%v auto_compress=true parallel=30"
		ctx := context.Background()
		if isStream {
			sqlText = fmt.Sprintf(
				sql, strings.ReplaceAll(fname, "\\", "\\\\"), tableName)
			rows = dbt.mustQueryContextT(WithFileStream(ctx, fileStream), t, sqlText)
		} else {
			sqlText = fmt.Sprintf(
				sql, strings.ReplaceAll(fname, "\\", "\\\\"), tableName)
			rows = dbt.mustQueryT(t, sqlText)
		}
		defer func() {
			assertNilF(t, rows.Close())
		}()

		var s0, s1, s2, s3, s4, s5, s6, s7 string
		assertTrueF(t, rows.Next(), "expected new rows")
		rows.mustScan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7)
		assertEqualF(t, s6, uploaded.String())
		// check file is PUT
		dbt.mustQueryAssertCount("ls @%"+tableName, 1)

		dbt.mustExecT(t, "copy into "+tableName)
		dbt.mustExecT(t, "rm @%"+tableName)
		dbt.mustQueryAssertCount("ls @%"+tableName, 0)

		dbt.mustExecT(t, fmt.Sprintf(`copy into @%%%v from %v file_format=(type=csv
			compression='gzip')`, tableName, tableName))

		var streamBuf bytes.Buffer
		if isStream {
			ctx = WithFileTransferOptions(ctx, &SnowflakeFileTransferOptions{GetFileToStream: true})
			ctx = WithFileGetStream(ctx, &streamBuf)
		}
		sql = fmt.Sprintf("get @%%%v 'file://%v' parallel=10", tableName, tmpDir)
		sqlText = strings.ReplaceAll(sql, "\\", "\\\\")
		rows2 := dbt.mustQueryContextT(ctx, t, sqlText)
		defer func() {
			assertNilF(t, rows2.Close())
		}()
		for rows2.Next() {
			rows2.mustScan(&s0, &s1, &s2, &s3)
			assertHasPrefixF(t, s0, "data_")
			v, err := strconv.Atoi(s1)
			assertNilF(t, err)
			assertEqualF(t, v, 36)
			assertEqualF(t, s2, "DOWNLOADED")
			assertEqualF(t, s3, "")
		}

		var contents string
		if isStream {
			gz, err := gzip.NewReader(&streamBuf)
			assertNilF(t, err)
			defer func() {
				assertNilF(t, gz.Close())
			}()
			for {
				c := make([]byte, defaultChunkBufferSize)
				if n, err := gz.Read(c); err != nil {
					if err == io.EOF {
						contents = contents + string(c[:n])
						break
					}
					t.Fatal(err)
				} else {
					contents = contents + string(c[:n])
				}
			}
		} else {
			files, err := filepath.Glob(filepath.Join(tmpDir, "data_*"))
			assertNilF(t, err)
			fileName := files[0]
			f, err := os.Open(fileName)
			assertNilF(t, err)
			defer func() {
				assertNilF(t, f.Close())
			}()

			gz, err := gzip.NewReader(f)
			assertNilF(t, err)
			defer func() {
				assertNilF(t, gz.Close())
			}()

			for {
				c := make([]byte, defaultChunkBufferSize)
				if n, err := gz.Read(c); err != nil {
					if err == io.EOF {
						contents = contents + string(c[:n])
						break
					}
					t.Fatal(err)
				} else {
					contents = contents + string(c[:n])
				}
			}
		}
		assertEqualE(t, contents, originalContents, "output is different from the original contents")
	})
}

func TestPutWithNonWritableTemp(t *testing.T) {
	if isWindows {
		t.Skip("permission system is different")
	}
	tempDir := t.TempDir()
	assertNilF(t, os.Chmod(tempDir, 0000))
	origDsn := dsn
	defer func() {
		dsn = origDsn
	}()
	dsn = dsn + "&tmpDirPath=" + strings.ReplaceAll(tempDir, "/", "%2F")
	runDBTest(t, func(dbt *DBTest) {
		for _, isStream := range []bool{false, true} {
			t.Run(fmt.Sprintf("isStream=%v", isStream), func(t *testing.T) {
				stageName := "test_stage_" + randomString(10)
				cwd, err := os.Getwd()
				assertNilF(t, err)
				filePath := fmt.Sprintf("%v/test_data/orders_100.csv", cwd)
				dbt.mustExecT(t, "CREATE STAGE "+stageName)
				defer dbt.mustExecT(t, "DROP STAGE "+stageName)

				ctx := context.Background()
				if isStream {
					fd, err := os.Open(filePath)
					assertNilF(t, err)
					ctx = WithFileStream(ctx, fd)
				}
				_, err = dbt.conn.ExecContext(ctx, fmt.Sprintf("PUT 'file://%v' @%v", filePath, stageName))
				if !isStream {
					assertNotNilF(t, err)
					assertStringContainsE(t, err.Error(), "mkdir")
					assertStringContainsE(t, err.Error(), "permission denied")
				} else {
					assertNilF(t, os.Chmod(tempDir, 0755))
					_ = dbt.mustExecContextT(ctx, t, fmt.Sprintf("GET @%v 'file://%v'", stageName, tempDir))
					resultBytesCompressed, err := os.ReadFile(filepath.Join(tempDir, "orders_100.csv.gz"))
					assertNilF(t, err)
					resultBytesReader, err := gzip.NewReader(bytes.NewReader(resultBytesCompressed))
					assertNilF(t, err)
					resultBytes, err := io.ReadAll(resultBytesReader)
					assertNilF(t, err)
					inputBytes, err := os.ReadFile(filePath)
					assertNilF(t, err)
					assertEqualE(t, string(resultBytes), string(inputBytes))
				}
			})
		}
	})
}

func TestGetWithNonWritableTemp(t *testing.T) {
	if isWindows {
		t.Skip("permission system is different")
	}
	tempDir := t.TempDir()
	origDsn := dsn
	defer func() {
		dsn = origDsn
	}()
	dsn = dsn + "&tmpDirPath=" + strings.ReplaceAll(tempDir, "/", "%2F")
	runDBTest(t, func(dbt *DBTest) {
		stageName := "test_stage_" + randomString(10)
		cwd, err := os.Getwd()
		assertNilF(t, err)
		filePath := fmt.Sprintf("%v/test_data/orders_100.csv", cwd)
		dbt.mustExecT(t, "CREATE STAGE "+stageName)
		defer dbt.mustExecT(t, "DROP STAGE "+stageName)

		dbt.mustExecT(t, fmt.Sprintf("PUT 'file://%v' @%v", filePath, stageName))
		assertNilF(t, os.Chmod(tempDir, 0000))

		for _, isStream := range []bool{false, true} {
			t.Run(fmt.Sprintf("isStream=%v", isStream), func(t *testing.T) {
				ctx := context.Background()
				var resultBuf bytes.Buffer
				if isStream {
					ctx = WithFileGetStream(ctx, &resultBuf)
					ctx = WithFileTransferOptions(ctx, &SnowflakeFileTransferOptions{GetFileToStream: true})
				}
				_, err = dbt.conn.ExecContext(ctx, fmt.Sprintf("GET @%v 'file://%v'", stageName, tempDir))
				if !isStream {
					assertNotNilF(t, err)
					assertStringContainsE(t, err.Error(), "mkdir")
					assertStringContainsE(t, err.Error(), "permission denied")
				} else {
					assertNilF(t, err)
					resultBytesReader, err := gzip.NewReader(&resultBuf)
					assertNilF(t, err)
					resultBytes, err := io.ReadAll(resultBytesReader)
					assertNilF(t, err)
					inputBytes, err := os.ReadFile(filePath)
					assertNilF(t, err)
					assertEqualE(t, string(resultBytes), string(inputBytes))
				}
			})
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
	defer func() {
		assertNilF(t, os.RemoveAll(tmpDir))
	}()
	fname := filepath.Join(tmpDir, "test_put_get.txt.gz")
	originalContents := "123,test1\n456,test2\n"
	tableName := randomString(5)

	var b bytes.Buffer
	gzw := gzip.NewWriter(&b)
	_, err = gzw.Write([]byte(originalContents))
	assertNilF(t, err)
	assertNilF(t, gzw.Close())
	if err = os.WriteFile(fname, b.Bytes(), readWriteFileMode); err != nil {
		t.Fatal("could not write to gzip file")
	}

	dsn = dsn + "&GCS_USE_DOWNSCOPED_CREDENTIAL=true"
	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("create or replace table " + tableName +
			" (a int, b string)")
		fileStream, err := os.Open(fname)
		if err != nil {
			t.Error(err)
		}
		defer func() {
			defer dbt.mustExec("drop table " + tableName)
			if fileStream != nil {
				assertNilF(t, fileStream.Close())
			}
		}()

		var sqlText string
		var rows *RowsExtended
		sql := "put 'file://%v' @%%%v auto_compress=true parallel=30"
		sqlText = fmt.Sprintf(
			sql, strings.ReplaceAll(fname, "\\", "\\\\"), tableName)
		rows = dbt.mustQuery(sqlText)
		defer func() {
			assertNilF(t, rows.Close())
		}()

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

		sql = fmt.Sprintf("get @%%%v 'file://%v'  parallel=10", tableName, tmpDir)
		sqlText = strings.ReplaceAll(sql, "\\", "\\\\")
		rows2 := dbt.mustQuery(sqlText)
		defer func() {
			assertNilF(t, rows2.Close())
		}()
		for rows2.Next() {
			if err = rows2.Scan(&s0, &s1, &s2, &s3); err != nil {
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

func TestPutGetLargeFile(t *testing.T) {
	sourceDir, err := os.Getwd()
	assertNilF(t, err)

	runDBTest(t, func(dbt *DBTest) {
		stageDir := "test_put_largefile_" + randomString(10)
		dbt.mustExec("rm @~/" + stageDir)

		// PUT test
		putQuery := fmt.Sprintf("put 'file://%v/test_data/largefile.txt' @~/%v", sourceDir, stageDir)
		sqlText := strings.ReplaceAll(putQuery, "\\", "\\\\")
		dbt.mustExec(sqlText)
		defer dbt.mustExec("rm @~/" + stageDir)
		rows := dbt.mustQuery("ls @~/" + stageDir)
		defer func() {
			assertNilF(t, rows.Close())
		}()
		var file, s1, s2, s3 string
		if rows.Next() {
			err = rows.Scan(&file, &s1, &s2, &s3)
			assertNilF(t, err)
		}

		if !strings.Contains(file, "largefile.txt.gz") {
			t.Fatalf("should contain file. got: %v", file)
		}

		// GET test with stream
		var streamBuf bytes.Buffer
		ctx := WithFileTransferOptions(context.Background(), &SnowflakeFileTransferOptions{GetFileToStream: true})
		ctx = WithFileGetStream(ctx, &streamBuf)
		sql := fmt.Sprintf("get @~/%v/largefile.txt.gz 'file://%v'", stageDir, t.TempDir())
		sqlText = strings.ReplaceAll(sql, "\\", "\\\\")
		rows2 := dbt.mustQueryContext(ctx, sqlText)
		defer func() {
			assertNilF(t, rows2.Close())
		}()
		for rows2.Next() {
			err = rows2.Scan(&file, &s1, &s2, &s3)
			assertNilE(t, err)
			assertTrueE(t, strings.HasPrefix(file, "largefile.txt.gz"), "a file was not downloaded by GET")
			v, err := strconv.Atoi(s1)
			assertNilE(t, err)
			assertEqualE(t, v, 424821, "did not return the right file size")
			assertEqualE(t, s2, "DOWNLOADED", "did not return DOWNLOADED status")
			assertEqualE(t, s3, "")
		}

		// convert the compressed stream to string
		var contents string
		gz, err := gzip.NewReader(&streamBuf)
		assertNilE(t, err)
		defer func() {
			assertNilF(t, gz.Close())
		}()
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

		// verify the downloaded stream with the original file
		fname := filepath.Join(sourceDir, "/test_data/largefile.txt")
		f, err := os.Open(fname)
		assertNilE(t, err)
		defer f.Close()
		originalContents, err := io.ReadAll(f)
		assertNilE(t, err)
		assertEqualF(t, contents, string(originalContents), "data did not match content")
	})
}

func TestPutGetMaxLOBSize(t *testing.T) {
	t.Skip("fails on CI because of backend testing in progress")

	testCases := [2]int{smallSize, largeSize}

	runDBTest(t, func(dbt *DBTest) {
		dbt.mustExec("alter session set ALLOW_LARGE_LOBS_IN_EXTERNAL_SCAN = false")
		defer dbt.mustExec("alter session unset ALLOW_LARGE_LOBS_IN_EXTERNAL_SCAN")
		for _, tc := range testCases {
			t.Run(strconv.Itoa(tc), func(t *testing.T) {

				// create the data file
				tmpDir := t.TempDir()
				fname := filepath.Join(tmpDir, "test_put_get.txt.gz")
				tableName := randomString(5)
				originalContents := fmt.Sprintf("%v,%s,%v\n", randomString(tc), randomString(tc), rand.Intn(100000))

				var b bytes.Buffer
				gzw := gzip.NewWriter(&b)
				_, err := gzw.Write([]byte(originalContents))
				assertNilF(t, err)
				assertNilF(t, gzw.Close())
				err = os.WriteFile(fname, b.Bytes(), readWriteFileMode)
				assertNilF(t, err, "could not write to gzip file")

				dbt.mustExec(fmt.Sprintf("create or replace table %s (c1 varchar, c2 varchar(%v), c3 int)", tableName, tc))
				defer dbt.mustExec("drop table " + tableName)
				fileStream, err := os.Open(fname)
				assertNilF(t, err)
				defer func() {
					assertNilF(t, fileStream.Close())
				}()

				// test PUT command
				var sqlText string
				var rows *RowsExtended
				sql := "put 'file://%v' @%%%v auto_compress=true parallel=30"
				sqlText = fmt.Sprintf(
					sql, strings.ReplaceAll(fname, "\\", "\\\\"), tableName)
				rows = dbt.mustQuery(sqlText)
				defer func() {
					assertNilF(t, rows.Close())
				}()

				var s0, s1, s2, s3, s4, s5, s6, s7 string
				assertTrueF(t, rows.Next(), "expected new rows")
				err = rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7)
				assertNilF(t, err)
				assertEqualF(t, s6, uploaded.String(), fmt.Sprintf("expected %v, got: %v", uploaded, s6))
				assertNilF(t, err)

				// check file is PUT
				dbt.mustQueryAssertCount("ls @%"+tableName, 1)

				dbt.mustExec("copy into " + tableName)
				dbt.mustExec("rm @%" + tableName)
				dbt.mustQueryAssertCount("ls @%"+tableName, 0)

				dbt.mustExec(fmt.Sprintf(`copy into @%%%v from %v file_format=(type=csv
			compression='gzip')`, tableName, tableName))

				// test GET command
				sql = fmt.Sprintf("get @%%%v 'file://%v'  parallel=10", tableName, tmpDir)
				sqlText = strings.ReplaceAll(sql, "\\", "\\\\")
				rows2 := dbt.mustQuery(sqlText)
				defer func() {
					assertNilF(t, rows2.Close())
				}()
				for rows2.Next() {
					err = rows2.Scan(&s0, &s1, &s2, &s3)
					assertNilE(t, err)
					assertTrueF(t, strings.HasPrefix(s0, "data_"), "a file was not downloaded by GET")
					assertEqualE(t, s2, "DOWNLOADED", "did not return DOWNLOADED status")
					assertEqualE(t, s3, "", fmt.Sprintf("returned %v", s3))
				}

				// verify the content in the file
				files, err := filepath.Glob(filepath.Join(tmpDir, "data_*"))
				assertNilF(t, err)

				fileName := files[0]
				f, err := os.Open(fileName)
				assertNilE(t, err)

				defer func() {
					assertNilF(t, f.Close())
				}()
				gz, err := gzip.NewReader(f)
				assertNilE(t, err)

				defer func() {
					assertNilF(t, gz.Close())
				}()
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
				assertEqualE(t, contents, originalContents, "output is different from the original file")
			})
		}
	})
}

func TestPutCancel(t *testing.T) {
	sourceDir, err := os.Getwd()
	assertNilF(t, err)

	testData, cleanup := createCancelTestFile(t, sourceDir)
	defer cleanup()

	stageDir := "test_put_cancel_" + randomString(10)

	runDBTest(t, func(dbt *DBTest) {
		c := make(chan error)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			_, err = dbt.conn.ExecContext(
				ctx,
				fmt.Sprintf("put 'file://%v' @~/%v overwrite=true",
					strings.ReplaceAll(testData, "\\", "/"), stageDir))
			c <- err
			close(c)
		}()
		time.Sleep(200 * time.Millisecond)
		cancel()
		ret := <-c
		assertNotNilF(t, ret)
		assertErrIsE(t, ret, context.Canceled)
	})
}

func TestPutGetLargeFileNonStream(t *testing.T) {
	testPutGetLargeFile(t, false, true)
}

func TestPutGetLargeFileNonStreamAutoCompressFalse(t *testing.T) {
	testPutGetLargeFile(t, false, false)
}

func TestPutGetLargeFileStream(t *testing.T) {
	testPutGetLargeFile(t, true, true)
}

func TestPutGetLargeFileStreamAutoCompressFalse(t *testing.T) {
	testPutGetLargeFile(t, true, false)
}

func testPutGetLargeFile(t *testing.T, isStream bool, autoCompress bool) {
	sourceDir, err := os.Getwd()
	assertNilF(t, err)

	fname := createLimitedRealFile(t, sourceDir)

	baseName := filepath.Base(fname)
	fnameGet := baseName + ".gz"
	if !autoCompress {
		fnameGet = baseName
	}

	runDBTest(t, func(dbt *DBTest) {
		stageDir := "test_put_largefile_" + randomString(10)
		dbt.mustExec("rm @~/" + stageDir)

		ctx := context.Background()
		if isStream {
			f, err := os.Open(fname)
			assertNilF(t, err)
			defer func() {
				assertNilF(t, f.Close())
			}()
			ctx = WithFileStream(ctx, f)
		}

		// PUT test
		escapedFname := strings.ReplaceAll(fname, "\\", "\\\\")
		putQuery := fmt.Sprintf("put 'file://%v' @~/%v auto_compress=true overwrite=true", escapedFname, stageDir)
		if !autoCompress {
			putQuery = fmt.Sprintf("put 'file://%v' @~/%v auto_compress=false overwrite=true", escapedFname, stageDir)
		}

		// Record initial memory stats before PUT
		var startMemStats, endMemStats runtime.MemStats
		runtime.ReadMemStats(&startMemStats)

		// Execute PUT command
		_ = dbt.mustExecContext(ctx, putQuery)

		// Record memory stats after PUT
		runtime.ReadMemStats(&endMemStats)
		fmt.Printf("Memory used for PUT command: %d MB\n", (endMemStats.Alloc-startMemStats.Alloc)/1024/1024)

		defer dbt.mustExec("rm @~/" + stageDir)
		rows := dbt.mustQuery("ls @~/" + stageDir)
		defer func() {
			assertNilF(t, rows.Close())
		}()
		var file, s1, s2, s3 sql.NullString
		if rows.Next() {
			err = rows.Scan(&file, &s1, &s2, &s3)
			assertNilF(t, err)
		}

		if !strings.Contains(file.String, fnameGet) {
			t.Fatalf("should contain file. got: %v", file.String)
		}

		// GET test
		var streamBuf bytes.Buffer
		ctx = context.Background()
		if isStream {
			ctx = WithFileTransferOptions(ctx, &SnowflakeFileTransferOptions{GetFileToStream: true})
			ctx = WithFileGetStream(ctx, &streamBuf)
		}

		tmpDir := t.TempDir()
		tmpDirURL := strings.ReplaceAll(tmpDir, "\\", "/")
		sql := fmt.Sprintf("get @~/%v/%v 'file://%v'", stageDir, fnameGet, tmpDirURL)
		sqlText := strings.ReplaceAll(sql, "\\", "\\\\")
		rows2 := dbt.mustQueryContext(ctx, sqlText)
		defer func() {
			assertNilF(t, rows2.Close())
		}()
		for rows2.Next() {
			err = rows2.Scan(&file, &s1, &s2, &s3)
			assertNilE(t, err)
			assertTrueE(t, strings.HasPrefix(file.String, fnameGet), "a file was not downloaded by GET")
			assertEqualE(t, s2.String, "DOWNLOADED", "did not return DOWNLOADED status")
			assertEqualE(t, s3.String, "")
		}

		var r io.Reader
		if autoCompress {
			// convert the compressed contents to string
			if isStream {
				r, err = gzip.NewReader(&streamBuf)
				assertNilE(t, err)
			} else {
				downloadedFile := filepath.Join(tmpDir, fnameGet)
				f, err := os.Open(downloadedFile)
				assertNilE(t, err)
				defer func() {
					assertNilF(t, f.Close())
				}()
				r, err = gzip.NewReader(f)
				assertNilE(t, err)
			}
		} else {
			if isStream {
				r = bytes.NewReader(streamBuf.Bytes())
			} else {
				downloadedFile := filepath.Join(tmpDir, fnameGet)
				f, err := os.Open(downloadedFile)
				assertNilE(t, err)
				defer func() {
					assertNilF(t, f.Close())
				}()
				r = bufio.NewReader(f)
			}
		}

		hash := sha256.New()
		_, err = io.Copy(hash, r)
		assertNilE(t, err)
		downloadedChecksum := fmt.Sprintf("%x", hash.Sum(nil))

		originalFile, err := os.Open(fname)
		assertNilF(t, err)
		defer func() {
			assertNilF(t, originalFile.Close())
		}()

		originalHash := sha256.New()
		_, err = io.Copy(originalHash, originalFile)
		assertNilE(t, err)
		originalChecksum := fmt.Sprintf("%x", originalHash.Sum(nil))

		assertEqualF(t, downloadedChecksum, originalChecksum, "file integrity check failed - checksums don't match")
	})
}

func createLimitedRealFile(t *testing.T, sourceDir string) string {
	tmpDir := t.TempDir()
	tmpFile, err := os.CreateTemp(tmpDir, "largefile_*.txt")
	assertNilF(t, err)
	fname := tmpFile.Name()

	originalFile, err := os.Open(filepath.Join(sourceDir, "test_data/largefile.txt"))
	if err == nil {
		defer originalFile.Close()
		limitSize := int64(5 * 1024 * 1024) // 5MB
		limitedReader := io.LimitReader(originalFile, limitSize)
		_, err = io.Copy(tmpFile, limitedReader)
		assertNilF(t, err)
	} else {
		syntheticData := strings.Repeat("TPC-H benchmark order data: ", 190000) // ~5MB
		_, err = tmpFile.WriteString(syntheticData)
		assertNilF(t, err)
	}

	err = tmpFile.Close()
	assertNilF(t, err)

	return fname
}

func createCancelTestFile(t *testing.T, sourceDir string) (string, func()) {
	originalFile, err := os.Open(filepath.Join(sourceDir, "test_data/largefile.txt"))
	assertNilF(t, err)
	defer originalFile.Close()

	tmpFile, err := os.CreateTemp(sourceDir, "cancel_test_*.txt")
	assertNilF(t, err)
	fname := tmpFile.Name()

	limitSize := int64(10 * 1024 * 1024) // 10MB
	limitedReader := io.LimitReader(originalFile, limitSize)

	_, err = io.Copy(tmpFile, limitedReader)
	assertNilF(t, err)

	err = tmpFile.Close()
	assertNilF(t, err)

	cleanup := func() {
		os.Remove(fname)
	}

	return fname, cleanup
}
