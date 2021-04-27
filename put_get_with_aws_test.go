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
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestPutFileWithAWS(t *testing.T) {
	if runningOnGithubAction() && !runningOnAWS() {
		t.Skip("skipping non aws environment")
	}
	testPutWithAWS(t, false)
}

func TestPutStreamWithAWS(t *testing.T) {
	if runningOnGithubAction() && !runningOnAWS() {
		t.Skip("skipping non aws environment")
	}
	testPutWithAWS(t, true)
}

func testPutWithAWS(t *testing.T, isStream bool) {
	tmpDir, _ := ioutil.TempDir("", "aws_put")
	defer os.RemoveAll(tmpDir)
	fname := filepath.Join(tmpDir, "test_put_get_with_aws.txt.gz")
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
		dbt.mustExec("create or replace table " + tableName +
			" (a int, b string)")
		fileStream, _ := os.OpenFile(fname, os.O_RDONLY, os.ModePerm)
		defer func() {
			defer dbt.mustExec("drop table " + tableName)
			if fileStream != nil {
				fileStream.Close()
			}
		}()

		sql := "put 'file://%v' @%%%v auto_compress=true parallel=30"
		if isStream {
			sqlText := fmt.Sprintf(
				sql,
				strings.ReplaceAll(fname, "\\", "\\\\"),
				tableName)
			dbt.mustExecContext(WithFileStream(
				context.Background(), fileStream), sqlText)
		} else {
			sqlText := fmt.Sprintf(
				sql,
				strings.ReplaceAll(fname, "\\", "\\\\"),
				tableName)
			dbt.mustExec(sqlText)
		}
		// check file is PUT
		dbt.mustQueryAssertCount("ls @%"+tableName, 1)

		dbt.mustExec("copy into " + tableName)
		dbt.mustExec("rm @%" + tableName)
		dbt.mustQueryAssertCount("ls @%"+tableName, 0)

		dbt.mustExec(fmt.Sprintf("copy into @%%%v from %v file_format=("+
			"type=csv compression='gzip')", tableName, tableName))
		dbt.mustQueryAssertCount("ls @%"+tableName, 1)
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
