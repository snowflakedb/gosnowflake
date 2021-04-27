package gosnowflake

import (
	"context"
	"fmt"
	"io/ioutil"
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
	putGetUserStage(t, "", 5, 1, false)
}

func TestPutGetStreamSmallDataViaUserStage(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Skip("this test requires to change the internal parameter")
	}
	putGetUserStage(t, "", 1, 1, true)
}

func putGetUserStage(t *testing.T, tmpDir string, numberOfFiles int, numberOfLines int, isStream bool) {
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Fatal("no aws secret access key found")
	}
	tmpDir, _ = ioutil.TempDir(tmpDir, "data")
	tmpDir = generateKLinesOfNFiles(numberOfLines, numberOfFiles, false, tmpDir)
	defer os.RemoveAll(tmpDir)
	var files string
	if isStream {
		list, _ := ioutil.ReadDir(tmpDir)
		file := list[0].Name()
		files = filepath.Join(tmpDir, file)
	} else {
		files = filepath.Join(tmpDir, "file*")
	}

	runTests(t, dsn, func(dbt *DBTest) {
		stageName := fmt.Sprintf("%v_stage_%v_%v", dbname, numberOfFiles, numberOfLines)
		dbt.mustExec(fmt.Sprintf("create or replace table %v (aa int, dt date, ts timestamp, tsltz timestamp_ltz, tsntz timestamp_ntz, tstz timestamp_tz, pct float, ratio number(6,2))", dbname))
		userBucket := os.Getenv("SF_AWS_USER_BUCKET")
		if userBucket == "" {
			userBucket = fmt.Sprintf("sfc-dev1-regression/%v/reg", user)
		}
		dbt.mustExec(fmt.Sprintf("create or replace stage %v url='s3://%v}/%v-%v-%v' credentials = (AWS_KEY_ID='%v' AWS_SECRET_KEY='%v')", stageName, userBucket, stageName, numberOfFiles, numberOfLines, os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")))

		dbt.mustExec("alter session set disable_put_and_get_on_external_stage = false")
		dbt.mustExec("rm @" + stageName)
		var fs *os.File
		if isStream {
			fs, _ = os.OpenFile(files, os.O_RDONLY, os.ModePerm)
			ctx := WithFileStream(context.Background(), fs)
			dbt.mustExecContext(ctx, fmt.Sprintf("put 'file://%v' @%v", strings.ReplaceAll(files, "\\", "\\\\"), stageName))
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
		var cnt string
		if rows.Next() {
			rows.Scan(&cnt)
		}
		count, _ := strconv.Atoi(cnt)
		if count != numberOfFiles*numberOfLines {
			t.Errorf("count did not match expected number. count: %v, expected: %v", count, numberOfFiles*numberOfLines)
		}
	})
}
