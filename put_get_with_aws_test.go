// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

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
			s3://sfc-dev1-data/twitter/O1k/tweets/ credentials=(AWS_KEY_ID='%v'
			AWS_SECRET_KEY='%v') file_format=(skip_header=1 null_if=('')
			field_optionally_enclosed_by='\"')`,
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

func TestPutWithInvalidToken(t *testing.T) {
	if !runningOnAWS() {
		t.Skip("skipping non aws environment")
	}
	tmpDir, _ := ioutil.TempDir("", "aws_put")
	defer os.RemoveAll(tmpDir)
	fname := filepath.Join(tmpDir, "test_put_get_with_aws.txt.gz")
	originalContents := "123,test1\n456,test2\n"

	var b bytes.Buffer
	gzw := gzip.NewWriter(&b)
	gzw.Write([]byte(originalContents))
	gzw.Close()
	if err := ioutil.WriteFile(fname, b.Bytes(), os.ModePerm); err != nil {
		t.Fatal("could not write to gzip file")
	}

	config, _ := ParseDSN(dsn)
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}

	tableName := randomString(5)
	if _, err = sc.Exec("create or replace table "+tableName+
		" (a int, b string)", nil); err != nil {
		t.Fatal(err)
	}
	defer sc.Exec("drop table "+tableName, nil)

	jsonBody, _ := json.Marshal(execRequest{
		SQLText: fmt.Sprintf("put 'file://%v' @%%%v", fname, tableName),
	})
	headers := getHeaders()
	headers[httpHeaderAccept] = headerContentTypeApplicationJSON
	data, err := sc.rest.FuncPostQuery(
		sc.ctx, sc.rest, &url.Values{}, headers, jsonBody,
		sc.rest.RequestTimeout, getOrGenerateRequestIDFromContext(sc.ctx), sc.cfg)
	if err != nil {
		t.Fatal(err)
	}

	s3Util := new(snowflakeS3Util)
	client := s3Util.createClient(&data.Data.StageInfo, false).(*s3.Client)

	s3Loc := s3Util.extractBucketNameAndPath(data.Data.StageInfo.Location)
	s3Path := s3Loc.s3Path + baseName(fname) + ".gz"

	f, _ := os.Open(fname)
	defer f.Close()
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
	client = s3Util.createClient(&info, false).(*s3.Client)

	uploader = manager.NewUploader(client)
	if _, err = uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket: &s3Loc.bucketName,
		Key:    &s3Path,
		Body:   f,
	}); err == nil {
		t.Fatal("should have failed attempting to put with missing aws token")
	}
}

func TestPretendToPutButList(t *testing.T) {
	if runningOnGithubAction() && !runningOnAWS() {
		t.Skip("skipping non aws environment")
	}
	tmpDir, _ := ioutil.TempDir("", "aws_put")
	defer os.RemoveAll(tmpDir)
	fname := filepath.Join(tmpDir, "test_put_get_with_aws.txt.gz")
	originalContents := "123,test1\n456,test2\n"

	var b bytes.Buffer
	gzw := gzip.NewWriter(&b)
	gzw.Write([]byte(originalContents))
	gzw.Close()
	if err := ioutil.WriteFile(fname, b.Bytes(), os.ModePerm); err != nil {
		t.Fatal("could not write to gzip file")
	}

	config, _ := ParseDSN(dsn)
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}

	tableName := randomString(5)
	if _, err = sc.Exec("create or replace table "+tableName+
		" (a int, b string)", nil); err != nil {
		t.Fatal(err)
	}
	defer sc.Exec("drop table "+tableName, nil)

	jsonBody, _ := json.Marshal(execRequest{
		SQLText: fmt.Sprintf("put 'file://%v' @%%%v", fname, tableName),
	})
	headers := getHeaders()
	headers[httpHeaderAccept] = headerContentTypeApplicationJSON
	data, err := sc.rest.FuncPostQuery(
		sc.ctx, sc.rest, &url.Values{}, headers, jsonBody,
		sc.rest.RequestTimeout, getOrGenerateRequestIDFromContext(sc.ctx), sc.cfg)
	if err != nil {
		t.Fatal(err)
	}

	s3Util := new(snowflakeS3Util)
	client := s3Util.createClient(&data.Data.StageInfo, false).(*s3.Client)
	if _, err = client.ListBuckets(context.Background(),
		&s3.ListBucketsInput{}); err == nil {
		t.Fatal("list buckets should fail")
	}
}
