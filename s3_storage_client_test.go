package gosnowflake

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
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

type mockUploadObjectAPI func(ctx context.Context, params *transfermanager.UploadObjectInput, optFns ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error)

func (m mockUploadObjectAPI) UploadObject(
	ctx context.Context,
	params *transfermanager.UploadObjectInput,
	optFns ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error) {
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

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false, &snowflakeTelemetry{})
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
		encryptMeta:       testEncryptionMeta(),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: multiPartThreshold,
		},
		mockUploader: mockUploadObjectAPI(func(ctx context.Context, params *transfermanager.UploadObjectInput, optFns ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error) {
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

	err = new(remoteStorageUtil).uploadOneFile(context.Background(), &uploadMeta)
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
	err = new(remoteStorageUtil).uploadOneFile(context.Background(), &uploadMeta)
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

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false, &snowflakeTelemetry{})
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
		encryptMeta:       testEncryptionMeta(),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: multiPartThreshold,
		},
		mockUploader: mockUploadObjectAPI(func(ctx context.Context, params *transfermanager.UploadObjectInput, optFns ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error) {
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

	err = new(remoteStorageUtil).uploadOneFile(context.Background(), &uploadMeta)
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

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false, &snowflakeTelemetry{})
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
		encryptMeta:       testEncryptionMeta(),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: multiPartThreshold,
		},
		mockUploader: mockUploadObjectAPI(func(ctx context.Context, params *transfermanager.UploadObjectInput, optFns ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error) {
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

	err = new(remoteStorageUtil).uploadOneFile(context.Background(), &uploadMeta)
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
	if header, err := (&snowflakeS3Client{cfg: &Config{}}).getFileHeader(context.Background(), &meta, "file.txt"); header != nil || err == nil {
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
	if header, err := (&snowflakeS3Client{cfg: &Config{}}).getFileHeader(context.Background(), &meta, "file.txt"); header != nil || err == nil {
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

	header, err := (&snowflakeS3Client{cfg: &Config{}}).getFileHeader(context.Background(), &meta, "file.txt")
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

	_, err := (&snowflakeS3Client{cfg: &Config{}}).getFileHeader(context.Background(), &meta, "file.txt")
	if err != nil && err.Error() != "could not find file" {
		t.Error(err)
	}

	if meta.resStatus != notFoundFile {
		t.Fatalf("expected %v result status, got: %v", errStatus, meta.resStatus)
	}
}

type mockDownloadObjectAPI func(ctx context.Context, params *transfermanager.DownloadObjectInput, optFns ...func(*transfermanager.Options)) (*transfermanager.DownloadObjectOutput, error)

func (m mockDownloadObjectAPI) DownloadObject(
	ctx context.Context,
	params *transfermanager.DownloadObjectInput,
	optFns ...func(*transfermanager.Options)) (*transfermanager.DownloadObjectOutput, error) {
	return m(ctx, params, optFns...)
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

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false, &snowflakeTelemetry{})
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
			MultiPartThreshold: multiPartThreshold,
		},
		mockDownloader: mockDownloadObjectAPI(func(ctx context.Context, params *transfermanager.DownloadObjectInput, optFns ...func(*transfermanager.Options)) (*transfermanager.DownloadObjectOutput, error) {
			return nil, &smithy.GenericAPIError{
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
	err = new(remoteStorageUtil).downloadOneFile(context.Background(), &downloadMeta)
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

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false, &snowflakeTelemetry{})
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
			MultiPartThreshold: multiPartThreshold,
		},
		mockDownloader: mockDownloadObjectAPI(func(ctx context.Context, params *transfermanager.DownloadObjectInput, optFns ...func(*transfermanager.Options)) (*transfermanager.DownloadObjectOutput, error) {
			return nil, &smithy.GenericAPIError{
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
	err = new(remoteStorageUtil).downloadOneFile(context.Background(), &downloadMeta)
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

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false, &snowflakeTelemetry{})
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
			MultiPartThreshold: multiPartThreshold,
		},
		mockDownloader: mockDownloadObjectAPI(func(ctx context.Context, params *transfermanager.DownloadObjectInput, optFns ...func(*transfermanager.Options)) (*transfermanager.DownloadObjectOutput, error) {
			return nil, &smithy.GenericAPIError{
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
	err = new(remoteStorageUtil).downloadOneFile(context.Background(), &downloadMeta)
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

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false, &snowflakeTelemetry{})
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
			MultiPartThreshold: multiPartThreshold,
		},
		mockDownloader: mockDownloadObjectAPI(func(ctx context.Context, params *transfermanager.DownloadObjectInput, optFns ...func(*transfermanager.Options)) (*transfermanager.DownloadObjectOutput, error) {
			return nil, errors.New("Failed to upload file")
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
	err = new(remoteStorageUtil).downloadOneFile(context.Background(), &downloadMeta)
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

	azureCli, err := new(snowflakeAzureClient).createClient(&info, false, &snowflakeTelemetry{})
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
		encryptMeta:       testEncryptionMeta(),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: multiPartThreshold,
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

	err = new(remoteStorageUtil).uploadOneFile(context.Background(), &uploadMeta)
	if err == nil {
		t.Fatal("should have failed")
	}
}

func TestGetHeaderClientCastFail(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "sfc-customer-stage/rwyi-testacco/users/9220/",
		LocationType: "S3",
	}
	azureCli, err := new(snowflakeAzureClient).createClient(&info, false, &snowflakeTelemetry{})
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

	_, err = new(snowflakeS3Client).getFileHeader(context.Background(), &meta, "file.txt")
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

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false, &snowflakeTelemetry{})
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
		encryptMeta:       testEncryptionMeta(),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: multiPartThreshold,
		},
		mockUploader: mockUploadObjectAPI(func(ctx context.Context, params *transfermanager.UploadObjectInput, optFns ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error) {
			return &transfermanager.UploadObjectOutput{
				Location: aws.String("https://sfc-customer-stage/rwyi-testacco/users/9220/data1.txt.gz"),
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

	err = (&remoteStorageUtil{cfg: &Config{}}).uploadOneFileWithRetry(context.Background(), &uploadMeta)
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

	s3Cli, err := new(snowflakeS3Client).createClient(&info, false, &snowflakeTelemetry{})
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
		encryptMeta:       testEncryptionMeta(),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: multiPartThreshold,
		},
		mockUploader: mockUploadObjectAPI(func(ctx context.Context, params *transfermanager.UploadObjectInput, optFns ...func(*transfermanager.Options)) (*transfermanager.UploadObjectOutput, error) {
			return nil, errors.New("unexpected error uploading file")
		}),
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}

	uploadMeta.realSrcStream = uploadMeta.srcStream

	err = new(remoteStorageUtil).uploadOneFile(context.Background(), &uploadMeta)
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

func TestWriteAtBufferConcurrentWriteAt(t *testing.T) {
	const (
		numChunks = 256
		chunkSize = 4096
	)
	expected := make([]byte, numChunks*chunkSize)
	for i := range expected {
		expected[i] = byte(i % 253)
	}
	buf := newWriteAtBuffer([]byte{})
	var wg sync.WaitGroup
	// Launch in reverse so the buffer first grows at its far end.
	for chunk := numChunks - 1; chunk >= 0; chunk-- {
		wg.Go(func() {
			off := chunk * chunkSize
			n, err := buf.WriteAt(expected[off:off+chunkSize], int64(off))
			assertNilE(t, err, "WriteAt at offset", strconv.Itoa(off))
			assertEqualE(t, n, chunkSize, "WriteAt at offset", strconv.Itoa(off))
		})
	}
	wg.Wait()
	assertEqualF(t, len(buf.Bytes()), len(expected), "buffer length after concurrent WriteAt")
	assertTrueF(t, bytes.Equal(buf.Bytes(), expected), "concurrent WriteAt produced corrupted buffer")
}

func TestWriteAtBufferGrowsAmortized(t *testing.T) {
	const (
		numWrites = 1024
		writeSize = 1024
	)
	buf := newWriteAtBuffer([]byte{})
	chunk := make([]byte, writeSize)
	reallocs := 0
	for i := range numWrites {
		prevCap := cap(buf.Bytes())
		_, err := buf.WriteAt(chunk, int64(i*writeSize))
		assertNilF(t, err, "WriteAt")
		if cap(buf.Bytes()) != prevCap {
			reallocs++
		}
	}
	assertEqualF(t, len(buf.Bytes()), numWrites*writeSize, "buffer length after sequential WriteAt")
	// Geometric growth reallocates O(log n) times; exact-size growth reallocates once per write.
	assertTrueF(t, reallocs <= 16, "expected amortized growth, got", strconv.Itoa(reallocs), "reallocations for", strconv.Itoa(numWrites), "writes")
}

func TestNativeDownloadFileStreamConcurrentParts(t *testing.T) {
	const (
		numParts  = 8
		partSize  = 256 * 1024
		pieceSize = 16 * 1024
	)
	expected := make([]byte, numParts*partSize)
	for i := range expected {
		expected[i] = byte(i*7 + i/partSize)
	}
	info := execResponseStageInfo{
		Location:     "sfc-teststage/rwyitestacco/users/1234/",
		LocationType: "S3",
	}
	s3Cli, err := new(snowflakeS3Client).createClient(&info, false, &snowflakeTelemetry{})
	assertNilF(t, err, "createClient")
	// Mimic transfermanager: each part is written by its own goroutine, in pieces.
	mockDownloader := mockDownloadObjectAPI(func(ctx context.Context, params *transfermanager.DownloadObjectInput, optFns ...func(*transfermanager.Options)) (*transfermanager.DownloadObjectOutput, error) {
		var wg sync.WaitGroup
		errs := make(chan error, numParts)
		for part := range numParts {
			wg.Go(func() {
				for off := part * partSize; off < (part+1)*partSize; off += pieceSize {
					if _, err := params.WriterAt.WriteAt(expected[off:off+pieceSize], int64(off)); err != nil {
						errs <- err
						return
					}
				}
			})
		}
		wg.Wait()
		close(errs)
		if err := <-errs; err != nil {
			return nil, err
		}
		return &transfermanager.DownloadObjectOutput{}, nil
	})
	meta := &fileMetadata{
		name:              "stream.bin",
		stageLocationType: "S3",
		client:            s3Cli,
		stageInfo:         &info,
		srcFileName:       "stream.bin",
		mockDownloader:    mockDownloader,
	}
	util := &snowflakeS3Client{cfg: &Config{}}
	ctx := WithFileGetStream(context.Background(), &bytes.Buffer{})
	err = util.nativeDownloadFile(ctx, meta, "", 10, multiPartThreshold)
	assertNilF(t, err, "nativeDownloadFile")
	assertEqualF(t, meta.resStatus, downloaded, "result status")
	assertEqualF(t, meta.dstStream.Len(), len(expected), "streamed length")
	assertTrueF(t, bytes.Equal(meta.dstStream.Bytes(), expected), "streamed bytes mismatch")
}

func TestNativeDownloadFileStreamPresizedFromSrcFileSize(t *testing.T) {
	const (
		numParts  = 3
		partSize  = 256 * 1024
		pieceSize = 16 * 1024
	)
	expected := make([]byte, numParts*partSize)
	for i := range expected {
		expected[i] = byte(i * 13)
	}
	info := execResponseStageInfo{
		Location:     "sfc-teststage/rwyitestacco/users/1234/",
		LocationType: "S3",
	}
	s3Cli, err := new(snowflakeS3Client).createClient(&info, false, &snowflakeTelemetry{})
	assertNilF(t, err, "createClient")
	mockDownloader := mockDownloadObjectAPI(func(ctx context.Context, params *transfermanager.DownloadObjectInput, optFns ...func(*transfermanager.Options)) (*transfermanager.DownloadObjectOutput, error) {
		for off := 0; off < len(expected); off += pieceSize {
			if _, err := params.WriterAt.WriteAt(expected[off:off+pieceSize], int64(off)); err != nil {
				return nil, err
			}
		}
		return &transfermanager.DownloadObjectOutput{}, nil
	})
	meta := &fileMetadata{
		name:              "stream.bin",
		stageLocationType: "S3",
		client:            s3Cli,
		stageInfo:         &info,
		srcFileName:       "stream.bin",
		srcFileSize:       int64(len(expected)),
		mockDownloader:    mockDownloader,
	}
	util := &snowflakeS3Client{cfg: &Config{}}
	ctx := WithFileGetStream(context.Background(), &bytes.Buffer{})
	err = util.nativeDownloadFile(ctx, meta, "", 10, multiPartThreshold)
	assertNilF(t, err, "nativeDownloadFile")
	assertTrueF(t, bytes.Equal(meta.dstStream.Bytes(), expected), "streamed bytes mismatch")
	assertEqualF(t, cap(meta.dstStream.Bytes()), len(expected), "stream capacity should match source size")
}
