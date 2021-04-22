// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	usr "os/user"
	"path/filepath"
	"strings"
	"testing"
)

func TestPutError(t *testing.T) {
	if isWindows {
		t.Skip("permission model is different")
	}
	tmpDir, _ := ioutil.TempDir("", "putfiledir")
	defer os.RemoveAll(tmpDir)
	file1 := filepath.Join(tmpDir, "file1")
	remoteLocation := filepath.Join(tmpDir, "remote_loc")
	f, _ := os.OpenFile(file1, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	f.WriteString("test1")
	os.Chmod(file1, 0000)

	data := &execResponseData{
		Command:           "UPLOAD",
		AutoCompress:      false,
		SrcLocations:      []string{file1},
		SourceCompression: "none",
		StageInfo: execResponseStageInfo{
			Location:     remoteLocation,
			LocationType: "LOCAL_FS",
			Path:         "remote_loc",
		},
	}

	fta := &snowflakeFileTransferAgent{
		data: data,
		options: &SnowflakeFileTransferOptions{
			raisePutGetError: false,
		},
	}
	if err := fta.execute(); err != nil {
		t.Fatal(err)
	}
	if _, err := fta.result(); err != nil {
		t.Fatal(err)
	}

	fta = &snowflakeFileTransferAgent{
		data: data,
		options: &SnowflakeFileTransferOptions{
			raisePutGetError: true,
		},
	}
	if err := fta.execute(); err != nil {
		t.Fatal(err)
	}
	if _, err := fta.result(); err == nil {
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

func createTestData(dbt *DBTest) (*tcPutGetData, error) {
	keyID, _ := os.LookupEnv("AWS_ACCESS_KEY_ID")
	secretKey, _ := os.LookupEnv("AWS_SECRET_ACCESS_KEY")
	bucket, present := os.LookupEnv("SF_AWS_USER_BUCKET")
	if !present {
		usr, _ := usr.Current()
		bucket = fmt.Sprintf("sfc-dev1-regression/%v/reg", usr.Username)
	}
	uniqueName := randomString(10)
	database := fmt.Sprintf("%v_db", uniqueName)
	wh := fmt.Sprintf("%v_wh", uniqueName)

	dir, _ := os.Getwd()
	ret := tcPutGetData{
		dir,
		keyID,
		secretKey,
		fmt.Sprintf("%v_stage", uniqueName),
		wh,
		database,
		bucket,
	}

	if _, err := dbt.db.Exec("use role sysadmin"); err != nil {
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

func TestLoadS3(t *testing.T) {
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
		dbt.mustExec("use schema " + data.database + ".gotesting_schema")
		execQuery := "create or replace table tweets(created_at timestamp, " +
			"id number, id_str string, text string, source string, " +
			"in_reply_to_status_id number, " +
			"in_reply_to_status_id_str string, in_reply_to_user_id number, " +
			"in_reply_to_user_id_str string, " +
			"in_reply_to_screen_name string, user__id number, " +
			"user__id_str string, user__name string, " +
			"user__screen_name string, user__location string, " +
			"user__description string, user__url string, " +
			"user__entities__description__urls string, " +
			"user__protected string, user__followers_count number, " +
			"user__friends_count number, user__listed_count number, " +
			"user__created_at timestamp, user__favourites_count number, " +
			"user__utc_offset number, user__time_zone string, " +
			"user__geo_enabled string, user__verified string, " +
			"user__statuses_count number, user__lang string, " +
			"user__contributors_enabled string, user__is_translator string, " +
			"user__profile_background_color string, " +
			"user__profile_background_image_url string, " +
			"user__profile_background_image_url_https string, " +
			"user__profile_background_tile string, " +
			"user__profile_image_url string, " +
			"user__profile_image_url_https string, " +
			"user__profile_link_color string, " +
			"user__profile_sidebar_border_color string, " +
			"user__profile_sidebar_fill_color string, " +
			"user__profile_text_color string, " +
			"user__profile_use_background_image string, " +
			"user__default_profile string, " +
			"user__default_profile_image string, user__following string, " +
			"user__follow_request_sent string, user__notifications string, " +
			"geo string, coordinates string, place string, " +
			"contributors string, retweet_count number, " +
			"favorite_count number, entities__hashtags string, " +
			"entities__symbols string, entities__urls string, " +
			"entities__user_mentions string, favorited string, " +
			"retweeted string, lang string)"
		dbt.mustExec(execQuery)
		defer dbt.mustExec("drop table if exists tweets")
		dbt.mustQueryAssertCount("ls @%tweets", 0)

		rows := dbt.mustQuery(fmt.Sprintf("copy into tweets from "+
			"s3://sfc-dev1-data/twitter/O1k/tweets/ credentials=("+
			"AWS_KEY_ID='%v' AWS_SECRET_KEY='%v') file_format=("+
			"skip_header=1 null_if=('') field_optionally_enclosed_by='\"')",
			data.awsAccessKeyID, data.awsSecretAccessKey))
		var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9 string
		cnt := 0
		for rows.Next() {
			rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9)
			cnt++
		}
		if cnt != 1 {
			t.Fatal("copy into tweets did not set row count to 1")
		}
		if s0 != "s3://sfc-dev1-data/twitter/O1k/tweets/1.csv.gz" {
			t.Fatalf("got %v as file", s0)
		}
	})
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
			"create or replace table gotest_putget_t1 ("+
				"c1 STRING, c2 STRING, c3 STRING,c4 STRING, c5 STRING, "+
				"c6 STRING, c7 STRING, c8 STRING, c9 STRING) "+
				"stage_file_format = ( field_delimiter = '|' "+
				"error_on_column_count_mismatch=false) "+
				"stage_copy_options = (purge=false) "+
				"stage_location = (url = 's3://%v/%v' credentials = ("+
				"AWS_KEY_ID='%v' AWS_SECRET_KEY='%v'))",
			data.userBucket,
			data.stage,
			data.awsAccessKeyID,
			data.awsSecretAccessKey)
		dbt.mustExec(execQuery)
		defer dbt.mustExec("drop table if exists gotest_putget_t1")

		execQuery = fmt.Sprintf("put file://%v/test_data/orders_10*.csv "+
			"@%%gotest_putget_t1", data.dir)
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

		rows = dbt.mustQuery("select STATUS from information_schema" +
			".load_history where table_name='gotest_putget_t1'")
		if rows.Next() {
			rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9)
			if s1 != "LOADED" {
				t.Fatal("not loaded")
			}
		}
	})
}

func TestPutLoadFromUserStage(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		data, err := createTestData(dbt)
		if err != nil {
			t.Skip("snowflake admin account not accessible")
		}
		defer cleanupPut(dbt, data)
		dbt.mustExec("alter session set DISABLE_PUT_AND_GET_ON_EXTERNAL_STAGE=false")
		dbt.mustExec("use warehouse " + data.warehouse)
		dbt.mustExec("use schema " + data.database + ".gotesting_schema")

		execQuery := fmt.Sprintf(
			"create or replace stage %v url = 's3://%v/%v' credentials = ("+
				"AWS_KEY_ID='%v' AWS_SECRET_KEY='%v')",
			data.stage, data.userBucket, data.stage,
			data.awsAccessKeyID, data.awsSecretAccessKey)
		dbt.mustExec(execQuery)

		execQuery = "create or replace table gotest_putget_t2 (" +
			"c1 STRING, c2 STRING, c3 STRING,c4 STRING, c5 STRING, " +
			"c6 STRING, c7 STRING, c8 STRING, c9 STRING)"
		dbt.mustExec(execQuery)
		defer dbt.mustExec("drop table if exists gotest_putget_t2")
		defer dbt.mustExec("drop stage if exists " + data.stage)

		execQuery = fmt.Sprintf("put file://%v/test_data/orders_10*.csv @%v",
			data.dir, data.stage)
		dbt.mustExec(execQuery)
		dbt.mustQueryAssertCount("ls @%gotest_putget_t2", 0)

		rows := dbt.mustQuery(fmt.Sprintf("copy into gotest_putget_t2 from "+
			"@%v file_format = (field_delimiter = '|' "+
			"error_on_column_count_mismatch=false) purge=true", data.stage))
		var s0, s1, s2, s3, s4, s5 string
		var s6, s7, s8, s9 interface{}
		orders100 := fmt.Sprintf("s3://%v/%v/orders_100.csv.gz",
			data.userBucket, data.stage)
		orders101 := fmt.Sprintf("s3://%v/%v/orders_101.csv.gz",
			data.userBucket, data.stage)
		for rows.Next() {
			rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9)
			if s0 != orders100 && s0 != orders101 {
				t.Fatalf("copy did not load orders files. got: %v", s0)
			}
		}
		dbt.mustQueryAssertCount(fmt.Sprintf("ls @%v", data.stage), 0)
	})
}

func TestPutWithAutoCompressFalse(t *testing.T) {
	if runningOnGithubAction() && !runningOnAWS() {
		t.Skip("skipping non aws environment")
	}
	os.MkdirAll("data", os.ModePerm)
	testData := filepath.Join("data", "data.txt")
	defer os.RemoveAll("data")
	f, _ := os.OpenFile(testData, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	f.WriteString("test1,test2\ntest3,test4")
	f.Sync()
	defer f.Close()

	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("rm @~/test_put_uncompress_file")
		dbt.mustExec(fmt.Sprintf("put file://%v @~/test_put_uncompress_file auto_compress=FALSE", testData))
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
	tmpDir, _ := ioutil.TempDir("", "data")
	defer os.RemoveAll(tmpDir)
	testData := filepath.Join(tmpDir, "data.txt")
	f, _ := os.OpenFile(testData, os.O_CREATE|os.O_RDWR, os.ModePerm)
	f.WriteString("test1,test2\ntest3,test4\n")
	f.Close()

	runTests(t, dsn, func(dbt *DBTest) {
		var err error
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
		rows = dbt.mustQueryContext(
			WithFileStream(context.Background(), f),
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
