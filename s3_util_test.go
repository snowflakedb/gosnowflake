// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type tcBucketPath struct {
	in     string
	bucket string
	s3path string
}

func TestExtractBucketNameAndPath(t *testing.T) {
	s3util := new(snowflakeS3Util)
	testcases := []tcBucketPath{
		{"sfc-dev1-regression/test_sub_dir/", "sfc-dev1-regression", "test_sub_dir/"},
		{"sfc-dev1-regression/dir/test_stg/test_sub_dir/", "sfc-dev1-regression", "dir/test_stg/test_sub_dir/"},
		{"sfc-dev1-regression/", "sfc-dev1-regression", ""},
		{"sfc-dev1-regression//", "sfc-dev1-regression", "/"},
		{"sfc-dev1-regression///", "sfc-dev1-regression", "//"},
	}
	for _, test := range testcases {
		s3Loc := s3util.extractBucketNameAndPath(test.in)
		if s3Loc.bucketName != test.bucket {
			t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.bucket, s3Loc.bucketName)
		}
		if s3Loc.s3Path != test.s3path {
			t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.s3path, s3Loc.s3Path)
		}
	}
}

type mockUploadObjectAPI func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*manager.Uploader)) (*manager.UploadOutput, error)

func (m mockUploadObjectAPI) Upload(
	ctx context.Context,
	params *s3.PutObjectInput,
	optFns ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
	return m(ctx, params, optFns...)
}

func TestUploadOneFileToS3WSAEConnAborted(t *testing.T) {
	if !runningOnAWS() {
		t.Skip("skipping non aws environment")
	}
	info := execResponseStageInfo{
		Location:     "sfc-customer-stage/rwyi-testacco/users/9220/",
		LocationType: "S3",
	}
	initialParallel := int64(100)
	dir, _ := os.Getwd()

	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "S3",
		noSleepingTime:    true,
		parallel:          initialParallel,
		client:            new(snowflakeS3Util).createClient(&info, false),
		sha256Digest:      "123456789abcdef",
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		srcFileName:       path.Join(dir, "/test_data/put_get_1.txt"),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockUploader: mockUploadObjectAPI(func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			return nil, &smithy.GenericAPIError{
				Code:    errNoWsaeconnaborted,
				Message: "mock err, connection aborted",
			}
		}),
	}

	uploadMeta.realSrcFileName = uploadMeta.srcFileName
	fi, err := os.Stat(uploadMeta.srcFileName)
	if err != nil {
		t.Error(err)
	}
	uploadMeta.uploadSize = fi.Size()

	err = new(remoteStorageUtil).uploadOneFile(&uploadMeta)
	if err == nil {
		t.Error("should have raised an error")
	}
	if uploadMeta.lastMaxConcurrency == 0 {
		t.Fatalf("expected concurrency. got: 0")
	}
	if uploadMeta.lastMaxConcurrency != int(initialParallel/defaultMaxRetry) {
		t.Fatalf("expected last max concurrency to be: %v, got: %v",
			int(initialParallel/defaultMaxRetry), uploadMeta.lastMaxConcurrency)
	}

	initialParallel = 4
	uploadMeta.parallel = initialParallel
	err = new(remoteStorageUtil).uploadOneFile(&uploadMeta)
	if err == nil {
		t.Error("should have raised an error")
	}
	if uploadMeta.lastMaxConcurrency == 0 {
		t.Fatalf("expected no last max concurrency. got: %v",
			uploadMeta.lastMaxConcurrency)
	}
	if uploadMeta.lastMaxConcurrency != 1 {
		t.Fatalf("expected last max concurrency to be: 1, got: %v",
			uploadMeta.lastMaxConcurrency)
	}
}

func TestUploadOneFileToS3ConnReset(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "sfc-teststage/rwyitestacco/users/1234/",
		LocationType: "S3",
	}
	initialParallel := int64(100)
	dir, _ := os.Getwd()

	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "S3",
		noSleepingTime:    true,
		parallel:          initialParallel,
		client:            new(snowflakeS3Util).createClient(&info, false),
		sha256Digest:      "123456789abcdef",
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		srcFileName:       path.Join(dir, "/test_data/put_get_1.txt"),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockUploader: mockUploadObjectAPI(func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			return nil, &smithy.GenericAPIError{
				Code:    strconv.Itoa(-1),
				Message: "mock err, connection aborted",
			}
		}),
	}

	uploadMeta.realSrcFileName = uploadMeta.srcFileName
	fi, err := os.Stat(uploadMeta.srcFileName)
	if err != nil {
		t.Error(err)
	}
	uploadMeta.uploadSize = fi.Size()

	err = new(remoteStorageUtil).uploadOneFile(&uploadMeta)
	if err == nil {
		t.Error("should have raised an error")
	}
	if uploadMeta.lastMaxConcurrency != 0 {
		t.Fatalf("expected no concurrency. got: %v",
			uploadMeta.lastMaxConcurrency)
	}
}

func TestUploadFileWithS3UploadFailedError(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "sfc-teststage/rwyitestacco/users/1234/",
		LocationType: "S3",
	}
	initialParallel := int64(100)
	dir, _ := os.Getwd()

	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "S3",
		noSleepingTime:    true,
		parallel:          initialParallel,
		client:            new(snowflakeS3Util).createClient(&info, false),
		sha256Digest:      "123456789abcdef",
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		srcFileName:       path.Join(dir, "/test_data/put_get_1.txt"),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockUploader: mockUploadObjectAPI(func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			return nil, &smithy.GenericAPIError{
				Code: expiredToken,
				Message: "An error occurred (ExpiredToken) when calling the " +
					"operation: The provided token has expired.",
			}
		}),
	}

	uploadMeta.realSrcFileName = uploadMeta.srcFileName
	fi, err := os.Stat(uploadMeta.srcFileName)
	if err != nil {
		t.Error(err)
	}
	uploadMeta.uploadSize = fi.Size()

	err = new(remoteStorageUtil).uploadOneFile(&uploadMeta)
	if err != nil {
		t.Error(err)
	}
	if uploadMeta.resStatus != renewToken {
		t.Fatalf("expected %v result status, got: %v",
			renewToken, uploadMeta.resStatus)
	}
}

type mockHeaderAPI func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)

func (m mockHeaderAPI) HeadObject(
	ctx context.Context,
	params *s3.HeadObjectInput,
	optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return m(ctx, params, optFns...)
}

func TestGetHeadExpiryError(t *testing.T) {
	meta := fileMetadata{
		client:    s3.New(s3.Options{}),
		stageInfo: &execResponseStageInfo{Location: ""},
		mockHeader: mockHeaderAPI(func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			return nil, &smithy.GenericAPIError{
				Code: expiredToken,
			}
		}),
	}
	if header := new(snowflakeS3Util).getFileHeader(&meta, "file.txt"); header != nil {
		t.Fatalf("expected null header, got: %v", header)
	}
	if meta.resStatus != renewToken {
		t.Fatalf("expected %v result status, got: %v",
			renewToken, meta.resStatus)
	}
}

func TestGetHeaderUnexpectedError(t *testing.T) {
	meta := fileMetadata{
		client:    s3.New(s3.Options{}),
		stageInfo: &execResponseStageInfo{Location: ""},
		mockHeader: mockHeaderAPI(func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			return nil, &smithy.GenericAPIError{
				Code: "-1",
			}
		}),
	}
	if header := new(snowflakeS3Util).getFileHeader(&meta, "file.txt"); header != nil {
		t.Fatalf("expected null header, got: %v", header)
	}
	if meta.resStatus != errStatus {
		t.Fatalf("expected %v result status, got: %v", errStatus, meta.resStatus)
	}
}
