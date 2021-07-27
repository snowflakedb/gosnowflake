// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestPutGetFileWithAWS(t *testing.T) {
	if runningOnGithubAction() && !runningOnAWS() {
		t.Skip("skipping non aws environment")
	}
	testPutGetWithAWS(t, false)
}

func TestPutGetStreamWithAWS(t *testing.T) {
	if runningOnGithubAction() && !runningOnAWS() {
		t.Skip("skipping non aws environment")
	}
	testPutGetWithAWS(t, true)
}

func testPutGetWithAWS(t *testing.T, isStream bool) {
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
		var sqlText string
		if isStream {
			sqlText = fmt.Sprintf(
				sql, strings.ReplaceAll(fname, "\\", "\\\\"), tableName)
			dbt.mustExecContext(WithFileStream(
				context.Background(), fileStream), sqlText)
		} else {
			sqlText = fmt.Sprintf(
				sql, strings.ReplaceAll(fname, "\\", "\\\\"), tableName)
			dbt.mustExec(sqlText)
		}
		// check file is PUT
		dbt.mustQueryAssertCount("ls @%"+tableName, 1)

		dbt.mustExec("copy into " + tableName)
		dbt.mustExec("rm @%" + tableName)
		dbt.mustQueryAssertCount("ls @%"+tableName, 0)

		dbt.mustExec(fmt.Sprintf(`copy into @%%%v from %v file_format=(type=csv
			compression='gzip')`, tableName, tableName))

		var s0, s1, s2, s3 string
		sql = fmt.Sprintf("get @%%%v 'file://%v'", tableName, tmpDir)
		sqlText = strings.ReplaceAll(sql, "\\", "\\\\")
		rows := dbt.mustQuery(sqlText)
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
