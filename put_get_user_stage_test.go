package gosnowflake

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestPutGetFileSmallDataViaUserStage(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Skip("this test requires to change the internal parameter")
	}
	putGetUserStage(t, 5, 1, false)
}

func TestPutGetStreamSmallDataViaUserStage(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Skip("this test requires to change the internal parameter")
	}
	putGetUserStage(t, 1, 1, true)
}

func putGetUserStage(t *testing.T, numberOfFiles int, numberOfLines int, isStream bool) {
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Fatal("no aws secret access key found")
	}
	tmpDir, err := generateKLinesOfNFiles(numberOfLines, numberOfFiles, false, t.TempDir())
	if err != nil {
		t.Error(err)
	}
	var files string
	if isStream {
		list, err := os.ReadDir(tmpDir)
		if err != nil {
			t.Error(err)
		}
		file := list[0].Name()
		files = filepath.Join(tmpDir, file)
	} else {
		files = filepath.Join(tmpDir, "file*")
	}

	runDBTest(t, func(dbt *DBTest) {
		stageName := fmt.Sprintf("%v_stage_%v_%v", dbname, numberOfFiles, numberOfLines)
		sqlText := `create or replace table %v (aa int, dt date, ts timestamp,
			tsltz timestamp_ltz, tsntz timestamp_ntz, tstz timestamp_tz,
			pct float, ratio number(6,2))`
		dbt.mustExec(fmt.Sprintf(sqlText, dbname))
		userBucket := os.Getenv("SF_AWS_USER_BUCKET")
		if userBucket == "" {
			userBucket = fmt.Sprintf("sfc-eng-regression/%v/reg", username)
		}
		sqlText = `create or replace stage %v url='s3://%v}/%v-%v-%v'
			credentials = (AWS_KEY_ID='%v' AWS_SECRET_KEY='%v')`
		dbt.mustExec(fmt.Sprintf(sqlText, stageName, userBucket, stageName,
			numberOfFiles, numberOfLines, os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_SECRET_ACCESS_KEY")))

		dbt.mustExec("alter session set disable_put_and_get_on_external_stage = false")
		dbt.mustExec("rm @" + stageName)
		var fs *os.File
		if isStream {
			fs, _ = os.Open(files)
			dbt.mustExecContext(WithFileStream(context.Background(), fs),
				fmt.Sprintf("put 'file://%v' @%v", strings.ReplaceAll(
					files, "\\", "\\\\"), stageName))
		} else {
			dbt.mustExec(fmt.Sprintf("put 'file://%v' @%v ", strings.ReplaceAll(files, "\\", "\\\\"), stageName))
		}
		defer func() {
			if isStream {
				fs.Close()
			}
			dbt.mustExec("rm @" + stageName)
			dbt.mustExec("drop stage if exists " + stageName)
			dbt.mustExec("drop table if exists " + dbname)
		}()
		dbt.mustExec(fmt.Sprintf("copy into %v from @%v", dbname, stageName))

		rows := dbt.mustQuery("select count(*) from " + dbname)
		defer func() {
			assertNilF(t, rows.Close())
		}()
		var cnt string
		if rows.Next() {
			assertNilF(t, rows.Scan(&cnt))
		}
		count, err := strconv.Atoi(cnt)
		if err != nil {
			t.Error(err)
		}
		if count != numberOfFiles*numberOfLines {
			t.Errorf("count did not match expected number. count: %v, expected: %v", count, numberOfFiles*numberOfLines)
		}
	})
}

func TestPutLoadFromUserStage(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		data, err := createTestData(dbt)
		if err != nil {
			t.Skip("snowflake admin account not accessible")
		}
		defer cleanupPut(dbt, data)
		dbt.mustExec("alter session set DISABLE_PUT_AND_GET_ON_EXTERNAL_STAGE=false")
		dbt.mustExec("use warehouse " + data.warehouse)
		dbt.mustExec("use schema " + data.database + ".gotesting_schema")

		execQuery := fmt.Sprintf(
			`create or replace stage %v url = 's3://%v/%v' credentials = (
			AWS_KEY_ID='%v' AWS_SECRET_KEY='%v')`,
			data.stage, data.userBucket, data.stage,
			data.awsAccessKeyID, data.awsSecretAccessKey)
		dbt.mustExec(execQuery)

		execQuery = `create or replace table gotest_putget_t2 (c1 STRING,
			c2 STRING, c3 STRING,c4 STRING, c5 STRING, c6 STRING, c7 STRING,
			c8 STRING, c9 STRING)`
		dbt.mustExec(execQuery)
		defer dbt.mustExec("drop table if exists gotest_putget_t2")
		defer dbt.mustExec("drop stage if exists " + data.stage)

		execQuery = fmt.Sprintf("put file://%v/test_data/orders_10*.csv @%v",
			data.dir, data.stage)
		dbt.mustExec(execQuery)
		dbt.mustQueryAssertCount("ls @%gotest_putget_t2", 0)

		rows := dbt.mustQuery(fmt.Sprintf(`copy into gotest_putget_t2 from @%v
			file_format = (field_delimiter = '|' error_on_column_count_mismatch
			=false) purge=true`, data.stage))
		defer func() {
			assertNilF(t, rows.Close())
		}()
		var s0, s1, s2, s3, s4, s5 string
		var s6, s7, s8, s9 interface{}
		orders100 := fmt.Sprintf("s3://%v/%v/orders_100.csv.gz",
			data.userBucket, data.stage)
		orders101 := fmt.Sprintf("s3://%v/%v/orders_101.csv.gz",
			data.userBucket, data.stage)
		for rows.Next() {
			assertNilF(t, rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9))
			if s0 != orders100 && s0 != orders101 {
				t.Fatalf("copy did not load orders files. got: %v", s0)
			}
		}
		dbt.mustQueryAssertCount(fmt.Sprintf("ls @%v", data.stage), 0)
	})
}
