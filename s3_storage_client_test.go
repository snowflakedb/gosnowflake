// Copyright (c) 2021-2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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
	path   string
}

func TestExtractBucketNameAndPath(t *testing.T) {
	s3util := new(snowflakeS3Client)
	testcases := []tcBucketPath{
		{"sfc-eng-regression/test_sub_dir/", "sfc-eng-regression", "test_sub_dir/"},
		{"sfc-eng-regression/dir/test_stg/test_sub_dir/", "sfc-eng-regression", "dir/test_stg/test_sub_dir/"},
		{"sfc-eng-regression/", "sfc-eng-regression", ""},
		{"sfc-eng-regression//", "sfc-eng-regression", "/"},
		{"sfc-eng-regression///", "sfc-eng-regression", "//"},
	}
	for _, test := range testcases {
		t.Run(test.in, func(t *testing.T) {
			s3Loc, err := s3util.extractBucketNameAndPath(test.in)
			if err != nil {
				t.Error(err)
			}
			if s3Loc.bucketName != test.bucket {
				t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.bucket, s3Loc.bucketName)
			}
			if s3Loc.s3Path != test.path {
				t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.path, s3Loc.s3Path)
			}
		})
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
	info := execResponseStageInfo{
		Location:     "sfc-customer-stage/rwyi-testacco/users/9220/",
		LocationType: "S3",
	}
	initialParallel := int64(100)
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "S3",
		noSleepingTime:    false,
		parallel:          initialParallel,
		client:            s3Cli,
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
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		}}

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
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "S3",
		noSleepingTime:    true,
		parallel:          initialParallel,
		client:            s3Cli,
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
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
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
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "S3",
		noSleepingTime:    true,
		parallel:          initialParallel,
		client:            s3Cli,
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
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
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
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}
	if header, err := (&snowflakeS3Client{cfg: &Config{}}).getFileHeader(&meta, "file.txt"); header != nil || err == nil {
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
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}
	if header, err := (&snowflakeS3Client{cfg: &Config{}}).getFileHeader(&meta, "file.txt"); header != nil || err == nil {
		t.Fatalf("expected null header, got: %v", header)
	}
	if meta.resStatus != errStatus {
		t.Fatalf("expected %v result status, got: %v", errStatus, meta.resStatus)
	}
}

func TestGetHeaderNonApiError(t *testing.T) {
	meta := fileMetadata{
		client:    s3.New(s3.Options{}),
		stageInfo: &execResponseStageInfo{Location: ""},
		mockHeader: mockHeaderAPI(func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			return nil, errors.New("something went wrong here")
		}),
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}

	header, err := (&snowflakeS3Client{cfg: &Config{}}).getFileHeader(&meta, "file.txt")
	assertNilE(t, header, fmt.Sprintf("expected header to be nil, actual: %v", header))
	assertNotNilE(t, err, "expected err to not be nil")
	assertEqualE(t, meta.resStatus, errStatus, fmt.Sprintf("expected %v result status for non-APIerror, got: %v", errStatus, meta.resStatus))
}

func TestGetHeaderNotFoundError(t *testing.T) {
	meta := fileMetadata{
		client:    s3.New(s3.Options{}),
		stageInfo: &execResponseStageInfo{Location: ""},
		mockHeader: mockHeaderAPI(func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			return nil, &smithy.GenericAPIError{
				Code: notFound,
			}
		}),
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}

	_, err := (&snowflakeS3Client{cfg: &Config{}}).getFileHeader(&meta, "file.txt")
	if err != nil && err.Error() != "could not find file" {
		t.Error(err)
	}

	if meta.resStatus != notFoundFile {
		t.Fatalf("expected %v result status, got: %v", errStatus, meta.resStatus)
	}
}

type mockDownloadObjectAPI func(ctx context.Context, w io.WriterAt, params *s3.GetObjectInput, optFns ...func(*manager.Downloader)) (int64, error)

func (m mockDownloadObjectAPI) Download(
	ctx context.Context,
	w io.WriterAt,
	params *s3.GetObjectInput,
	optFns ...func(*manager.Downloader)) (int64, error) {
	return m(ctx, w, params, optFns...)
}

func TestDownloadFileWithS3TokenExpired(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "sfc-teststage/rwyitestacco/users/1234/",
		LocationType: "S3",
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	downloadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "S3",
		noSleepingTime:    true,
		client:            s3Cli,
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		overwrite:         true,
		srcFileName:       "data1.txt.gz",
		localLocation:     dir,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockDownloader: mockDownloadObjectAPI(func(ctx context.Context, w io.WriterAt, params *s3.GetObjectInput, optFns ...func(*manager.Downloader)) (int64, error) {
			return 0, &smithy.GenericAPIError{
				Code: expiredToken,
				Message: "An error occurred (ExpiredToken) when calling the " +
					"operation: The provided token has expired.",
			}
		}),
		mockHeader: mockHeaderAPI(func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			return &s3.HeadObjectOutput{}, nil
		}),
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}
	err = new(remoteStorageUtil).downloadOneFile(&downloadMeta)
	if err == nil {
		t.Error("should have raised an error")
	}
	if downloadMeta.resStatus != renewToken {
		t.Fatalf("expected %v result status, got: %v",
			renewToken, downloadMeta.resStatus)
	}
}

func TestDownloadFileWithS3ConnReset(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "sfc-teststage/rwyitestacco/users/1234/",
		LocationType: "S3",
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	downloadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "S3",
		noSleepingTime:    true,
		client:            s3Cli,
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		overwrite:         true,
		srcFileName:       "data1.txt.gz",
		localLocation:     dir,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockDownloader: mockDownloadObjectAPI(func(ctx context.Context, w io.WriterAt, params *s3.GetObjectInput, optFns ...func(*manager.Downloader)) (int64, error) {
			return 0, &smithy.GenericAPIError{
				Code:    strconv.Itoa(-1),
				Message: "mock err, connection aborted",
			}
		}),
		mockHeader: mockHeaderAPI(func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			return &s3.HeadObjectOutput{}, nil
		}),
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}
	err = new(remoteStorageUtil).downloadOneFile(&downloadMeta)
	if err == nil {
		t.Error("should have raised an error")
	}
	if downloadMeta.lastMaxConcurrency != 0 {
		t.Fatalf("expected no concurrency. got: %v",
			downloadMeta.lastMaxConcurrency)
	}
}

func TestDownloadOneFileToS3WSAEConnAborted(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "sfc-teststage/rwyitestacco/users/1234/",
		LocationType: "S3",
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	downloadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "S3",
		noSleepingTime:    true,
		client:            s3Cli,
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		overwrite:         true,
		srcFileName:       "data1.txt.gz",
		localLocation:     dir,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockDownloader: mockDownloadObjectAPI(func(ctx context.Context, w io.WriterAt, params *s3.GetObjectInput, optFns ...func(*manager.Downloader)) (int64, error) {
			return 0, &smithy.GenericAPIError{
				Code:    errNoWsaeconnaborted,
				Message: "mock err, connection aborted",
			}
		}),
		mockHeader: mockHeaderAPI(func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			return &s3.HeadObjectOutput{}, nil
		}),
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}
	err = new(remoteStorageUtil).downloadOneFile(&downloadMeta)
	if err == nil {
		t.Error("should have raised an error")
	}

	if downloadMeta.resStatus != needRetryWithLowerConcurrency {
		t.Fatalf("expected %v result status, got: %v",
			needRetryWithLowerConcurrency, downloadMeta.resStatus)
	}
}

func TestDownloadOneFileToS3Failed(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "sfc-teststage/rwyitestacco/users/1234/",
		LocationType: "S3",
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	downloadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "S3",
		noSleepingTime:    true,
		client:            s3Cli,
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		overwrite:         true,
		srcFileName:       "data1.txt.gz",
		localLocation:     dir,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockDownloader: mockDownloadObjectAPI(func(ctx context.Context, w io.WriterAt, params *s3.GetObjectInput, optFns ...func(*manager.Downloader)) (int64, error) {
			return 0, errors.New("Failed to upload file")
		}),
		mockHeader: mockHeaderAPI(func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			return &s3.HeadObjectOutput{}, nil
		}),
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}
	err = new(remoteStorageUtil).downloadOneFile(&downloadMeta)
	if err == nil {
		t.Error("should have raised an error")
	}

	if downloadMeta.resStatus != needRetry {
		t.Fatalf("expected %v result status, got: %v",
			needRetry, downloadMeta.resStatus)
	}
}

func TestUploadFileToS3ClientCastFail(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "sfc-customer-stage/rwyi-testacco/users/9220/",
		LocationType: "S3",
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	azureCli, err := new(snowflakeAzureClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "S3",
		noSleepingTime:    false,
		client:            azureCli,
		sha256Digest:      "123456789abcdef",
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		srcFileName:       path.Join(dir, "/test_data/put_get_1.txt"),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}

	uploadMeta.realSrcFileName = uploadMeta.srcFileName
	fi, err := os.Stat(uploadMeta.srcFileName)
	if err != nil {
		t.Error(err)
	}
	uploadMeta.uploadSize = fi.Size()

	err = new(remoteStorageUtil).uploadOneFile(&uploadMeta)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestGetHeaderClientCastFail(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "sfc-customer-stage/rwyi-testacco/users/9220/",
		LocationType: "S3",
	}
	azureCli, err := new(snowflakeAzureClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	meta := fileMetadata{
		client:    azureCli,
		stageInfo: &execResponseStageInfo{Location: ""},
		mockHeader: mockHeaderAPI(func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			return nil, &smithy.GenericAPIError{
				Code: notFound,
			}
		}),
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}

	_, err = new(snowflakeS3Client).getFileHeader(&meta, "file.txt")
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestS3UploadRetryWithHeaderNotFound(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "sfc-customer-stage/rwyi-testacco/users/9220/",
		LocationType: "S3",
	}
	initialParallel := int64(100)
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "S3",
		noSleepingTime:    false,
		parallel:          initialParallel,
		client:            s3Cli,
		sha256Digest:      "123456789abcdef",
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		srcFileName:       path.Join(dir, "/test_data/put_get_1.txt"),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockUploader: mockUploadObjectAPI(func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			return &manager.UploadOutput{
				Location: "https://sfc-customer-stage/rwyi-testacco/users/9220/data1.txt.gz",
			}, nil
		}),
		mockHeader: mockHeaderAPI(func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			return nil, &smithy.GenericAPIError{
				Code: notFound,
			}
		}),
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}

	uploadMeta.realSrcFileName = uploadMeta.srcFileName
	fi, err := os.Stat(uploadMeta.srcFileName)
	if err != nil {
		t.Error(err)
	}
	uploadMeta.uploadSize = fi.Size()

	err = (&remoteStorageUtil{cfg: &Config{}}).uploadOneFileWithRetry(&uploadMeta)
	if err != nil {
		t.Error(err)
	}

	if uploadMeta.resStatus != errStatus {
		t.Fatalf("expected %v result status, got: %v", errStatus, uploadMeta.resStatus)
	}
}

func TestS3UploadStreamFailed(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "sfc-customer-stage/rwyi-testacco/users/9220/",
		LocationType: "S3",
	}
	initialParallel := int64(100)
	src := []byte{65, 66, 67}

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "S3",
		noSleepingTime:    true,
		parallel:          initialParallel,
		client:            s3Cli,
		sha256Digest:      "123456789abcdef",
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		srcStream:         bytes.NewBuffer(src),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockUploader: mockUploadObjectAPI(func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
			return nil, errors.New("unexpected error uploading file")
		}),
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}

	uploadMeta.realSrcStream = uploadMeta.srcStream

	err = new(remoteStorageUtil).uploadOneFile(&uploadMeta)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestConvertContentLength(t *testing.T) {
	someInt := int64(1)
	tcs := []struct {
		contentLength any
		desc          string
		expected      int64
	}{
		{
			contentLength: someInt,
			desc:          "int",
			expected:      1,
		},
		{
			contentLength: &someInt,
			desc:          "pointer",
			expected:      1,
		},
		{
			contentLength: float64(1),
			desc:          "another type",
			expected:      0,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.desc, func(t *testing.T) {
			actual := convertContentLength(tc.contentLength)
			assertEqualF(t, actual, tc.expected, fmt.Sprintf("expected %v (%T) but got %v (%T)", actual, actual, tc.expected, tc.expected))
		})
	}
}

func TestGetS3Endpoint(t *testing.T) {
	testcases := []struct {
		desc string
		in   execResponseStageInfo
		out  string
	}{

		{
			desc: "when UseRegionalURL is valid and the region does not start with cn-",
			in: execResponseStageInfo{
				UseS3RegionalURL: false,
				UseRegionalURL:   true,
				EndPoint:         "",
				Region:           "WEST-1",
			},
			out: "https://s3.WEST-1.amazonaws.com",
		},
		{
			desc: "when UseS3RegionalURL is valid and the region does not start with cn-",
			in: execResponseStageInfo{
				UseS3RegionalURL: true,
				UseRegionalURL:   false,
				EndPoint:         "",
				Region:           "WEST-1",
			},
			out: "https://s3.WEST-1.amazonaws.com",
		},
		{
			desc: "when endPoint is enabled and the region does not start with cn-",
			in: execResponseStageInfo{
				UseS3RegionalURL: false,
				UseRegionalURL:   false,
				EndPoint:         "s3.endpoint",
				Region:           "mockLocation",
			},
			out: "https://s3.endpoint",
		},
		{
			desc: "when endPoint is enabled and the region starts with cn-",
			in: execResponseStageInfo{
				UseS3RegionalURL: false,
				UseRegionalURL:   false,
				EndPoint:         "s3.endpoint",
				Region:           "cn-mockLocation",
			},
			out: "https://s3.endpoint",
		},
		{
			desc: "when useS3RegionalURL is valid and domain starts with cn",
			in: execResponseStageInfo{
				UseS3RegionalURL: true,
				UseRegionalURL:   false,
				EndPoint:         "",
				Region:           "cn-mockLocation",
			},
			out: "https://s3.cn-mockLocation.amazonaws.com.cn",
		},
		{
			desc: "when useRegionalURL is valid and domain starts with cn",
			in: execResponseStageInfo{
				UseS3RegionalURL: true,
				UseRegionalURL:   false,
				EndPoint:         "",
				Region:           "cn-mockLocation",
			},
			out: "https://s3.cn-mockLocation.amazonaws.com.cn",
		},
		{
			desc: "when useRegionalURL is valid and domain starts with cn",
			in: execResponseStageInfo{
				UseS3RegionalURL: true,
				UseRegionalURL:   false,
				EndPoint:         "",
				Region:           "cn-mockLocation",
			},
			out: "https://s3.cn-mockLocation.amazonaws.com.cn",
		},
		{
			desc: "when endPoint is specified, both UseRegionalURL and useS3PRegionalUrl are valid, and the region starts with cn",
			in: execResponseStageInfo{
				UseS3RegionalURL: true,
				UseRegionalURL:   true,
				EndPoint:         "s3.endpoint",
				Region:           "cn-mockLocation",
			},
			out: "https://s3.endpoint",
		},
	}

	for _, test := range testcases {
		t.Run(test.desc, func(t *testing.T) {
			endpoint := getS3CustomEndpoint(&test.in)
			if *endpoint != test.out {
				t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.out, *endpoint)
			}
		})
	}
}
