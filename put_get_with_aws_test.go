// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestLoadS3(t *testing.T) {
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
		dbt.mustExec("use schema " + data.database + ".gotesting_schema")
		execQuery := `create or replace table tweets(created_at timestamp,
			id number, id_str string, text string, source string,
			in_reply_to_status_id number, in_reply_to_status_id_str string,
			in_reply_to_user_id number, in_reply_to_user_id_str string,
			in_reply_to_screen_name string, user__id number, user__id_str string,
			user__name string, user__screen_name string, user__location string,
			user__description string, user__url string,
			user__entities__description__urls string, user__protected string,
			user__followers_count number, user__friends_count number,
			user__listed_count number, user__created_at timestamp,
			user__favourites_count number, user__utc_offset number,
			user__time_zone string, user__geo_enabled string,
			user__verified string, user__statuses_count number, user__lang string,
			user__contributors_enabled string, user__is_translator string,
			user__profile_background_color string,
			user__profile_background_image_url string,
			user__profile_background_image_url_https string,
			user__profile_background_tile string, user__profile_image_url string,
			user__profile_image_url_https string, user__profile_link_color string,
			user__profile_sidebar_border_color string,
			user__profile_sidebar_fill_color string, user__profile_text_color string,
			user__profile_use_background_image string, user__default_profile string,
			user__default_profile_image string, user__following string,
			user__follow_request_sent string, user__notifications string,
			geo string, coordinates string, place string, contributors string,
			retweet_count number, favorite_count number, entities__hashtags string,
			entities__symbols string, entities__urls string,
			entities__user_mentions string, favorited string, retweeted string,
			lang string)`
		dbt.mustExec(execQuery)
		defer dbt.mustExec("drop table if exists tweets")
		dbt.mustQueryAssertCount("ls @%tweets", 0)

		rows := dbt.mustQuery(fmt.Sprintf(`copy into tweets from
			s3://sfc-eng-data/twitter/O1k/tweets/ credentials=(AWS_KEY_ID='%v'
			AWS_SECRET_KEY='%v') file_format=(skip_header=1 null_if=('')
			field_optionally_enclosed_by='\"')`,
			data.awsAccessKeyID, data.awsSecretAccessKey))
		defer func() {
			assertNilF(t, rows.Close())
		}()
		var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9 sql.NullString
		cnt := 0
		for rows.Next() {
			assertNilF(t, rows.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9))
			cnt++
		}
		if cnt != 1 {
			t.Fatal("copy into tweets did not set row count to 1")
		}
		if !s0.Valid || s0.String != "s3://sfc-eng-data/twitter/O1k/tweets/1.csv.gz" {
			t.Fatalf("got %v as file", s0)
		}
	})
}

func TestPutWithInvalidToken(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		if !runningOnAWS() {
			t.Skip("skipping non aws environment")
		}
		tmpDir := t.TempDir()
		fname := filepath.Join(tmpDir, "test_put_get_with_aws.txt.gz")
		originalContents := "123,test1\n456,test2\n"

		var b bytes.Buffer
		gzw := gzip.NewWriter(&b)
		_, err := gzw.Write([]byte(originalContents))
		assertNilF(t, err)
		assertNilF(t, gzw.Close())
		if err := os.WriteFile(fname, b.Bytes(), readWriteFileMode); err != nil {
			t.Fatal("could not write to gzip file")
		}

		tableName := randomString(5)
		sct.mustExec("create or replace table "+tableName+" (a int, b string)", nil)
		defer sct.mustExec("drop table "+tableName, nil)

		jsonBody, err := json.Marshal(execRequest{
			SQLText: fmt.Sprintf("put 'file://%v' @%%%v", fname, tableName),
		})
		if err != nil {
			t.Error(err)
		}
		headers := getHeaders()
		headers[httpHeaderAccept] = headerContentTypeApplicationJSON
		data, err := sct.sc.rest.FuncPostQuery(
			sct.sc.ctx, sct.sc.rest, &url.Values{}, headers, jsonBody,
			sct.sc.rest.RequestTimeout, getOrGenerateRequestIDFromContext(sct.sc.ctx), sct.sc.cfg)
		if err != nil {
			t.Fatal(err)
		}

		s3Util := new(snowflakeS3Client)
		s3Cli, err := s3Util.createClient(&data.Data.StageInfo, false)
		if err != nil {
			t.Error(err)
		}
		client := s3Cli.(*s3.Client)

		s3Loc, err := s3Util.extractBucketNameAndPath(data.Data.StageInfo.Location)
		if err != nil {
			t.Error(err)
		}
		s3Path := s3Loc.s3Path + baseName(fname) + ".gz"

		f, err := os.Open(fname)
		if err != nil {
			t.Error(err)
		}
		defer func() {
			assertNilF(t, f.Close())
		}()
		uploader := manager.NewUploader(client)
		if _, err = uploader.Upload(context.Background(), &s3.PutObjectInput{
			Bucket: &s3Loc.bucketName,
			Key:    &s3Path,
			Body:   f,
		}); err != nil {
			t.Fatal(err)
		}

		parentPath := filepath.Dir(filepath.Dir(s3Path)) + "/"
		if _, err = uploader.Upload(context.Background(), &s3.PutObjectInput{
			Bucket: &s3Loc.bucketName,
			Key:    &parentPath,
			Body:   f,
		}); err == nil {
			t.Fatal("should have failed attempting to put file in parent path")
		}

		info := execResponseStageInfo{
			Creds: execResponseCredentials{
				AwsID:        data.Data.StageInfo.Creds.AwsID,
				AwsSecretKey: data.Data.StageInfo.Creds.AwsSecretKey,
			},
		}
		s3Cli, err = s3Util.createClient(&info, false)
		if err != nil {
			t.Error(err)
		}
		client = s3Cli.(*s3.Client)

		uploader = manager.NewUploader(client)
		if _, err = uploader.Upload(context.Background(), &s3.PutObjectInput{
			Bucket: &s3Loc.bucketName,
			Key:    &s3Path,
			Body:   f,
		}); err == nil {
			t.Fatal("should have failed attempting to put with missing aws token")
		}
	})
}

func TestPretendToPutButList(t *testing.T) {
	if runningOnGithubAction() && !runningOnAWS() {
		t.Skip("skipping non aws environment")
	}
	tmpDir := t.TempDir()
	fname := filepath.Join(tmpDir, "test_put_get_with_aws.txt.gz")
	originalContents := "123,test1\n456,test2\n"

	var b bytes.Buffer
	gzw := gzip.NewWriter(&b)
	_, err := gzw.Write([]byte(originalContents))
	assertNilF(t, err)
	assertNilF(t, gzw.Close())
	if err := os.WriteFile(fname, b.Bytes(), readWriteFileMode); err != nil {
		t.Fatal("could not write to gzip file")
	}

	runSnowflakeConnTest(t, func(sct *SCTest) {
		tableName := randomString(5)
		sct.mustExec("create or replace table "+tableName+
			" (a int, b string)", nil)
		defer sct.mustExec("drop table "+tableName, nil)

		jsonBody, err := json.Marshal(execRequest{
			SQLText: fmt.Sprintf("put 'file://%v' @%%%v", fname, tableName),
		})
		if err != nil {
			t.Error(err)
		}
		headers := getHeaders()
		headers[httpHeaderAccept] = headerContentTypeApplicationJSON
		data, err := sct.sc.rest.FuncPostQuery(
			sct.sc.ctx, sct.sc.rest, &url.Values{}, headers, jsonBody,
			sct.sc.rest.RequestTimeout, getOrGenerateRequestIDFromContext(sct.sc.ctx), sct.sc.cfg)
		if err != nil {
			t.Fatal(err)
		}

		s3Util := new(snowflakeS3Client)
		s3Cli, err := s3Util.createClient(&data.Data.StageInfo, false)
		if err != nil {
			t.Error(err)
		}
		client := s3Cli.(*s3.Client)
		if _, err = client.ListBuckets(context.Background(),
			&s3.ListBucketsInput{}); err == nil {
			t.Fatal("list buckets should fail")
		}
	})
}

func TestPutGetAWSStage(t *testing.T) {
	if runningOnGithubAction() || !runningOnAWS() {
		t.Skip("skipping non aws environment")
	}

	tmpDir := t.TempDir()
	name := "test_put_get.txt.gz"
	fname := filepath.Join(tmpDir, name)
	originalContents := "123,test1\n456,test2\n"
	stageName := "test_put_get_stage_" + randomString(5)

	var b bytes.Buffer
	gzw := gzip.NewWriter(&b)
	_, err := gzw.Write([]byte(originalContents))
	assertNilF(t, err)
	assertNilF(t, gzw.Close())
	if err := os.WriteFile(fname, b.Bytes(), readWriteFileMode); err != nil {
		t.Fatal("could not write to gzip file")
	}

	runDBTest(t, func(dbt *DBTest) {
		var createStageQuery string
		keyID, secretKey, _, err := getAWSCredentials()
		if err != nil {
			t.Skip("snowflake admin account not accessible")
		}
		createStageQuery = fmt.Sprintf(createStageStmt,
			stageName,
			"s3://"+stageName,
			fmt.Sprintf("AWS_KEY_ID='%v' AWS_SECRET_KEY='%v'", keyID, secretKey))
		dbt.mustExec(createStageQuery)

		defer dbt.mustExec("DROP STAGE IF EXISTS " + stageName)

		sql := "put 'file://%v' @~/%v auto_compress=false"
		sqlText := fmt.Sprintf(sql, strings.ReplaceAll(fname, "\\", "\\\\"), stageName)
		rows := dbt.mustQuery(sqlText)
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

		sql = fmt.Sprintf("get @~/%v 'file://%v'", stageName, tmpDir)
		sqlText = strings.ReplaceAll(sql, "\\", "\\\\")
		rows = dbt.mustQuery(sqlText)
		defer func() {
			assertNilF(t, rows.Close())
		}()
		for rows.Next() {
			if err = rows.Scan(&s0, &s1, &s2, &s3); err != nil {
				t.Error(err)
			}

			if strings.Compare(s0, name) != 0 {
				t.Error("a file was not downloaded by GET")
			}
			if v, err := strconv.Atoi(s1); err != nil || v != 41 {
				t.Error("did not return the right file size")
			}
			if s2 != "DOWNLOADED" {
				t.Error("did not return DOWNLOADED status")
			}
			if s3 != "" {
				t.Errorf("returned %v", s3)
			}
		}

		files, err := filepath.Glob(filepath.Join(tmpDir, "*"))
		if err != nil {
			t.Fatal(err)
		}
		fileName := files[0]
		f, err := os.Open(fileName)
		if err != nil {
			t.Error(err)
		}
		defer func() {
			assertNilF(t, f.Close())
		}()
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
