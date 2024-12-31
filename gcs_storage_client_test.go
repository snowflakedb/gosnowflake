// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path"
	"testing"
)

type tcFileURL struct {
	location string
	fname    string
	bucket   string
	filepath string
}

func TestExtractBucketAndPath(t *testing.T) {
	gcsUtil := new(snowflakeGcsClient)
	testcases := []tcBucketPath{
		{"sfc-eng-regression/test_sub_dir/", "sfc-eng-regression", "test_sub_dir/"},
		{"sfc-eng-regression/dir/test_stg/test_sub_dir/", "sfc-eng-regression", "dir/test_stg/test_sub_dir/"},
		{"sfc-eng-regression/", "sfc-eng-regression", ""},
		{"sfc-eng-regression//", "sfc-eng-regression", "/"},
		{"sfc-eng-regression///", "sfc-eng-regression", "//"},
	}
	for _, test := range testcases {
		t.Run(test.in, func(t *testing.T) {
			gcsLoc := gcsUtil.extractBucketNameAndPath(test.in)
			if gcsLoc.bucketName != test.bucket {
				t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.bucket, gcsLoc.bucketName)
			}
			if gcsLoc.path != test.path {
				t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.path, gcsLoc.path)
			}
		})
	}
}

func TestIsTokenExpiredWith401(t *testing.T) {
	gcsUtil := new(snowflakeGcsClient)
	dd := &execResponseData{}
	execResp := &execResponse{
		Data:    *dd,
		Message: "token expired",
		Code:    "401",
		Success: true,
	}
	ba, err := json.Marshal(execResp)
	if err != nil {
		panic(err)
	}
	resp := &http.Response{StatusCode: http.StatusUnauthorized, Body: &fakeResponseBody{body: ba}}
	if !gcsUtil.isTokenExpired(resp) {
		t.Fatalf("expected true for token expired")
	}
}

func TestIsTokenExpiredWith404(t *testing.T) {
	gcsUtil := new(snowflakeGcsClient)
	dd := &execResponseData{}
	execResp := &execResponse{
		Data:    *dd,
		Message: "file not found",
		Code:    "404",
		Success: true,
	}
	ba, err := json.Marshal(execResp)
	if err != nil {
		panic(err)
	}
	resp := &http.Response{StatusCode: http.StatusNotFound, Body: &fakeResponseBody{body: ba}}
	if gcsUtil.isTokenExpired(resp) {
		t.Fatalf("should be false")
	}
	resp = &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}}}

	if gcsUtil.isTokenExpired(resp) {
		t.Fatalf("should be false")
	}
	resp = &http.Response{
		StatusCode: http.StatusUnauthorized,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}}}

	if !gcsUtil.isTokenExpired(resp) {
		t.Fatalf("should be true")
	}
}

func TestGenerateFileURL(t *testing.T) {
	gcsUtil := new(snowflakeGcsClient)
	testcases := []tcFileURL{
		{"sfc-eng-regression/test_sub_dir/", "file1", "sfc-eng-regression", "test_sub_dir/file1"},
		{"sfc-eng-regression/dir/test_stg/test_sub_dir/", "file2", "sfc-eng-regression", "dir/test_stg/test_sub_dir/file2"},
		{"sfc-eng-regression/", "file3", "sfc-eng-regression", "file3"},
		{"sfc-eng-regression//", "file4", "sfc-eng-regression", "/file4"},
		{"sfc-eng-regression///", "file5", "sfc-eng-regression", "//file5"},
	}
	for _, test := range testcases {
		t.Run(test.location, func(t *testing.T) {
			stageInfo := &execResponseStageInfo{}
			stageInfo.Location = test.location
			gcsURL, err := gcsUtil.generateFileURL(stageInfo, test.fname)
			if err != nil {
				t.Error(err)
			}
			expectedURL, err := url.Parse("https://storage.googleapis.com/" + test.bucket + "/" + url.QueryEscape(test.filepath))
			if err != nil {
				t.Error(err)
			}
			if gcsURL.String() != expectedURL.String() {
				t.Fatalf("failed. expected: %v but got: %v", expectedURL.String(), gcsURL.String())
			}
		})
	}
}

type clientMock struct {
	DoFunc func(req *http.Request) (*http.Response, error)
}

func (c *clientMock) Do(req *http.Request) (*http.Response, error) {
	return c.DoFunc(req)
}

func TestUploadFileWithGcsUploadFailedError(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs-blob/storage/users/456/",
		LocationType: "GCS",
	}
	initialParallel := int64(100)
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:               "data1.txt.gz",
		stageLocationType:  "GCS",
		noSleepingTime:     true,
		parallel:           initialParallel,
		client:             gcsCli,
		sha256Digest:       "123456789abcdef",
		stageInfo:          &info,
		dstFileName:        "data1.txt.gz",
		srcFileName:        path.Join(dir, "/test_data/put_get_1.txt"),
		overwrite:          true,
		dstCompressionType: compressionTypes["GZIP"],
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return nil, errors.New("unexpected error uploading file")
			},
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

func TestUploadFileWithGcsUploadFailedWithRetry(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs-blob/storage/users/456/",
		LocationType: "GCS",
	}
	encMat := snowflakeFileEncryption{
		QueryStageMasterKey: "abCdEFO0upIT36dAxGsa0w==",
		QueryID:             "01abc874-0406-1bf0-0000-53b10668e056",
		SMKID:               92019681909886,
	}
	initialParallel := int64(100)
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:               "data1.txt.gz",
		stageLocationType:  "GCS",
		noSleepingTime:     true,
		parallel:           initialParallel,
		client:             gcsCli,
		sha256Digest:       "123456789abcdef",
		stageInfo:          &info,
		dstFileName:        "data1.txt.gz",
		srcFileName:        path.Join(dir, "/test_data/put_get_1.txt"),
		overwrite:          true,
		dstCompressionType: compressionTypes["GZIP"],
		encryptionMaterial: &encMat,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     "403 Forbidden",
					StatusCode: 403,
				}, nil
			},
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
		t.Error("should have raised an error")
	}

	if uploadMeta.resStatus != needRetry {
		t.Fatalf("expected %v result status, got: %v",
			needRetry, uploadMeta.resStatus)
	}
}

func TestUploadFileWithGcsUploadFailedWithTokenExpired(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs-blob/storage/users/456/",
		LocationType: "GCS",
		Creds: execResponseCredentials{
			GcsAccessToken: "test-token-124456577",
		},
	}
	initialParallel := int64(100)
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "GCS",
		noSleepingTime:    true,
		parallel:          initialParallel,
		client:            gcsCli,
		sha256Digest:      "123456789abcdef",
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		srcFileName:       path.Join(dir, "/test_data/put_get_1.txt"),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     "401 Unauthorized",
					StatusCode: 401,
				}, nil
			},
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
	if err != nil {
		t.Error(err)
	}

	if uploadMeta.resStatus != renewToken {
		t.Fatalf("expected %v result status, got: %v",
			renewToken, uploadMeta.resStatus)
	}
}

func TestDownloadOneFileFromGcsFailed(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	downloadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "GCS",
		noSleepingTime:    true,
		client:            gcsCli,
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		overwrite:         true,
		srcFileName:       "data1.txt.gz",
		localLocation:     dir,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return nil, errors.New("unexpected error downloading file")
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
		resStatus: downloaded, // bypass file header request
	}
	err = new(remoteStorageUtil).downloadOneFile(&downloadMeta)
	if err == nil {
		t.Error("should have raised an error")
	}
}

func TestDownloadOneFileFromGcsFailedWithRetry(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	downloadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "GCS",
		noSleepingTime:    true,
		client:            gcsCli,
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		overwrite:         true,
		srcFileName:       "data1.txt.gz",
		localLocation:     dir,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     "403 Forbidden",
					StatusCode: 403,
				}, nil
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
		resStatus: downloaded, // bypass file header request
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

func TestDownloadOneFileFromGcsFailedWithTokenExpired(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
		Creds: execResponseCredentials{
			GcsAccessToken: "test-token-124456577",
		},
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	downloadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "GCS",
		noSleepingTime:    true,
		client:            gcsCli,
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		overwrite:         true,
		srcFileName:       "data1.txt.gz",
		localLocation:     dir,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     "401 Unauthorized",
					StatusCode: 401,
				}, nil
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
		resStatus: downloaded, // bypass file header request
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

func TestDownloadOneFileFromGcsFailedWithFileNotFound(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
		Creds: execResponseCredentials{
			GcsAccessToken: "test-token-124456577",
		},
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	downloadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "GCS",
		noSleepingTime:    true,
		client:            gcsCli,
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		overwrite:         true,
		srcFileName:       "data1.txt.gz",
		localLocation:     dir,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     "404 Not Found",
					StatusCode: 404,
				}, nil
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
		resStatus: downloaded, // bypass file header request
	}
	err = new(remoteStorageUtil).downloadOneFile(&downloadMeta)
	if err == nil {
		t.Error("should have raised an error")
	}

	if downloadMeta.resStatus != notFoundFile {
		t.Fatalf("expected %v result status, got: %v",
			notFoundFile, downloadMeta.resStatus)
	}
}

func TestGetHeaderTokenExpiredError(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
		Creds: execResponseCredentials{
			GcsAccessToken: "test-token-124456577",
		},
	}
	meta := fileMetadata{
		client:    info.Creds.GcsAccessToken,
		stageInfo: &info,
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     "401 Unauthorized",
					StatusCode: 401,
				}, nil
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}
	if header, err := (&snowflakeGcsClient{cfg: &Config{}}).getFileHeader(&meta, "file.txt"); header != nil || err == nil {
		t.Fatalf("expected null header, got: %v", header)
	}
	if meta.resStatus != renewToken {
		t.Fatalf("expected %v result status, got: %v",
			renewToken, meta.resStatus)
	}
}

func TestGetHeaderFileNotFound(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
		Creds: execResponseCredentials{
			GcsAccessToken: "test-token-124456577",
		},
	}
	meta := fileMetadata{
		client:    info.Creds.GcsAccessToken,
		stageInfo: &info,
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     "404 Not Found",
					StatusCode: 404,
				}, nil
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}
	if header, err := (&snowflakeGcsClient{cfg: &Config{}}).getFileHeader(&meta, "file.txt"); header != nil || err == nil {
		t.Fatalf("expected null header, got: %v", header)
	}
	if meta.resStatus != notFoundFile {
		t.Fatalf("expected %v result status, got: %v",
			notFoundFile, meta.resStatus)
	}
}

func TestGetHeaderPresignedUrlReturns404(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
		Creds: execResponseCredentials{
			GcsAccessToken: "test-token-124456577",
		},
	}
	presignedURL, err := url.Parse("https://google-cloud.test.com")
	if err != nil {
		t.Error(err)
	}
	meta := fileMetadata{
		client:       info.Creds.GcsAccessToken,
		stageInfo:    &info,
		presignedURL: presignedURL,
	}
	header, err := (&snowflakeGcsClient{cfg: &Config{}}).getFileHeader(&meta, "file.txt")
	if header != nil {
		t.Fatalf("expected null header, got: %v", header)
	}
	if err != nil {
		t.Error(err)
	}
	if meta.resStatus != notFoundFile {
		t.Fatalf("expected %v result status, got: %v",
			notFoundFile, meta.resStatus)
	}
}

func TestGetHeaderReturnsError(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
		Creds: execResponseCredentials{
			GcsAccessToken: "test-token-124456577",
		},
	}
	meta := fileMetadata{
		client:    info.Creds.GcsAccessToken,
		stageInfo: &info,
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return nil, errors.New("unexpected exception getting file header")
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}
	if header, err := (&snowflakeGcsClient{cfg: &Config{}}).getFileHeader(&meta, "file.txt"); header != nil || err == nil {
		t.Fatalf("expected null header, got: %v", header)
	}
}

func TestGetHeaderBadRequest(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
		Creds: execResponseCredentials{
			GcsAccessToken: "test-token-124456577",
		},
	}
	meta := fileMetadata{
		client:    info.Creds.GcsAccessToken,
		stageInfo: &info,
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     "400 Bad Request",
					StatusCode: 400,
				}, nil
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}
	if header, err := (&snowflakeGcsClient{cfg: &Config{}}).getFileHeader(&meta, "file.txt"); header != nil || err == nil {
		t.Fatalf("expected null header, got: %v", header)
	}

	if meta.resStatus != errStatus {
		t.Fatalf("expected %v result status, got: %v",
			errStatus, meta.resStatus)
	}
}

func TestGetHeaderRetryableError(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
		Creds: execResponseCredentials{
			GcsAccessToken: "test-token-124456577",
		},
	}
	meta := fileMetadata{
		client:    info.Creds.GcsAccessToken,
		stageInfo: &info,
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     "403 Forbidden",
					StatusCode: 403,
				}, nil
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}
	if header, err := (&snowflakeGcsClient{cfg: &Config{}}).getFileHeader(&meta, "file.txt"); header != nil || err == nil {
		t.Fatalf("expected null header, got: %v", header)
	}
	if meta.resStatus != needRetry {
		t.Fatalf("expected %v result status, got: %v",
			needRetry, meta.resStatus)
	}
}

func TestUploadStreamFailed(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs-blob/storage/users/456/",
		LocationType: "GCS",
	}
	initialParallel := int64(100)
	src := []byte{65, 66, 67}

	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "GCS",
		noSleepingTime:    true,
		parallel:          initialParallel,
		client:            gcsCli,
		sha256Digest:      "123456789abcdef",
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		srcStream:         bytes.NewBuffer(src),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return nil, errors.New("unexpected error uploading file")
			},
		},
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

func TestUploadFileWithBadRequest(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs-blob/storage/users/456/",
		LocationType: "GCS",
	}
	initialParallel := int64(100)
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "GCS",
		noSleepingTime:    true,
		parallel:          initialParallel,
		client:            gcsCli,
		sha256Digest:      "123456789abcdef",
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		srcFileName:       path.Join(dir, "/test_data/put_get_1.txt"),
		overwrite:         true,
		lastError:         nil,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: 400,
				}, nil
			},
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
	if err != nil {
		t.Error(err)
	}

	if uploadMeta.resStatus != renewPresignedURL {
		t.Fatalf("expected %v result status, got: %v",
			renewPresignedURL, uploadMeta.resStatus)
	}
}

func TestGetFileHeaderEncryptionData(t *testing.T) {
	mockEncDataResp := "{\"EncryptionMode\":\"FullBlob\",\"WrappedContentKey\": {\"KeyId\":\"symmKey1\",\"EncryptedKey\":\"testencryptedkey12345678910==\",\"Algorithm\":\"AES_CBC_256\"},\"EncryptionAgent\": {\"Protocol\":\"1.0\",\"EncryptionAlgorithm\":\"AES_CBC_256\"},\"ContentEncryptionIV\":\"testIVkey12345678910==\",\"KeyWrappingMetadata\":{\"EncryptionLibrary\":\"Java 5.3.0\"}}"
	mockMatDesc := "{\"queryid\":\"01abc874-0406-1bf0-0000-53b10668e056\",\"smkid\":\"92019681909886\",\"key\":\"128\"}"
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
		Creds: execResponseCredentials{
			GcsAccessToken: "test-token-124456577",
		},
	}
	meta := fileMetadata{
		client:    info.Creds.GcsAccessToken,
		stageInfo: &info,
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     "200 OK",
					StatusCode: 200,
					Header: http.Header{
						"X-Goog-Meta-Encryptiondata": []string{mockEncDataResp},
						"Content-Length":             []string{"4256"},
						"X-Goog-Meta-Sfc-Digest":     []string{"123456789abcdef"},
						"X-Goog-Meta-Matdesc":        []string{mockMatDesc},
					},
				}, nil
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}
	header, err := (&snowflakeGcsClient{cfg: &Config{}}).getFileHeader(&meta, "file.txt")
	if err != nil {
		t.Fatal(err)
	}
	expectedFileHeader := &fileHeader{
		digest:        "123456789abcdef",
		contentLength: 4256,
		encryptionMetadata: &encryptMetadata{
			key:     "testencryptedkey12345678910==",
			iv:      "testIVkey12345678910==",
			matdesc: mockMatDesc,
		},
	}
	if header.contentLength != expectedFileHeader.contentLength || header.digest != expectedFileHeader.digest || header.encryptionMetadata.iv != expectedFileHeader.encryptionMetadata.iv || header.encryptionMetadata.key != expectedFileHeader.encryptionMetadata.key || header.encryptionMetadata.matdesc != expectedFileHeader.encryptionMetadata.matdesc {
		t.Fatalf("unexpected file header. expected: %v, got: %v", expectedFileHeader, header)
	}
}

func TestGetFileHeaderEncryptionDataInterfaceConversionError(t *testing.T) {
	mockEncDataResp := "{\"EncryptionMode\":\"FullBlob\",\"WrappedContentKey\": {\"KeyId\":\"symmKey1\",\"EncryptedKey\":\"testencryptedkey12345678910==\",\"Algorithm\":\"AES_CBC_256\"},\"EncryptionAgent\": {\"Protocol\":\"1.0\",\"EncryptionAlgorithm\":\"AES_CBC_256\"},\"ContentEncryptionIV\":\"testIVkey12345678910==\",\"KeyWrappingMetadata\":{\"EncryptionLibrary\":\"Java 5.3.0\"}}"
	mockMatDesc := "{\"queryid\":\"01abc874-0406-1bf0-0000-53b10668e056\",\"smkid\":\"92019681909886\",\"key\":\"128\"}"
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
		Creds: execResponseCredentials{
			GcsAccessToken: "test-token-124456577",
		},
	}
	meta := fileMetadata{
		client:    1,
		stageInfo: &info,
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     "200 OK",
					StatusCode: 200,
					Header: http.Header{
						"X-Goog-Meta-Encryptiondata": []string{mockEncDataResp},
						"Content-Length":             []string{"4256"},
						"X-Goog-Meta-Sfc-Digest":     []string{"123456789abcdef"},
						"X-Goog-Meta-Matdesc":        []string{mockMatDesc},
					},
				}, nil
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}
	_, err := (&snowflakeGcsClient{cfg: &Config{}}).getFileHeader(&meta, "file.txt")
	if err == nil {
		t.Error("should have raised an error")
	}
}

func TestUploadFileToGcsNoStatus(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs-blob/storage/users/456/",
		LocationType: "GCS",
	}
	encMat := snowflakeFileEncryption{
		QueryStageMasterKey: "abCdEFO0upIT36dAxGsa0w==",
		QueryID:             "01abc874-0406-1bf0-0000-53b10668e056",
		SMKID:               92019681909886,
	}
	initialParallel := int64(100)
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:               "data1.txt.gz",
		stageLocationType:  "GCS",
		noSleepingTime:     true,
		parallel:           initialParallel,
		client:             gcsCli,
		sha256Digest:       "123456789abcdef",
		stageInfo:          &info,
		dstFileName:        "data1.txt.gz",
		srcFileName:        path.Join(dir, "/test_data/put_get_1.txt"),
		overwrite:          true,
		dstCompressionType: compressionTypes["GZIP"],
		encryptionMaterial: &encMat,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     "401 Unauthorized",
					StatusCode: 401,
				}, nil
			},
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
		t.Error("should have raised an error")
	}
}

func TestDownloadFileFromGcsError(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	downloadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "GCS",
		noSleepingTime:    true,
		client:            gcsCli,
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		overwrite:         true,
		srcFileName:       "data1.txt.gz",
		localLocation:     dir,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     "403 Unauthorized",
					StatusCode: 401,
				}, nil
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
		resStatus: downloaded, // bypass file header request
	}
	err = new(remoteStorageUtil).downloadOneFile(&downloadMeta)
	if err == nil {
		t.Error("should have raised an error")
	}
}

func TestDownloadFileWithBadRequest(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	downloadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "GCS",
		noSleepingTime:    true,
		client:            gcsCli,
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		overwrite:         true,
		srcFileName:       "data1.txt.gz",
		localLocation:     dir,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					Status:     "400 Bad Request",
					StatusCode: 400,
				}, nil
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
		resStatus: downloaded, // bypass file header request
	}
	err = new(remoteStorageUtil).downloadOneFile(&downloadMeta)
	if err == nil {
		t.Error("should have raised an error")
	}

	if downloadMeta.resStatus != renewPresignedURL {
		t.Fatalf("expected %v result status, got: %v",
			renewPresignedURL, downloadMeta.resStatus)
	}
}

func Test_snowflakeGcsClient_uploadFile(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
		Creds: execResponseCredentials{
			GcsAccessToken: "test-token-124456577",
		},
	}
	meta := fileMetadata{
		client:    1,
		stageInfo: &info,
	}
	err := new(snowflakeGcsClient).uploadFile("somedata", &meta, nil, 1, 1)
	if err == nil {
		t.Error("should have raised an error")
	}
}

func Test_snowflakeGcsClient_nativeDownloadFile(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
		Creds: execResponseCredentials{
			GcsAccessToken: "test-token-124456577",
		},
	}
	meta := fileMetadata{
		client:    1,
		stageInfo: &info,
	}
	err := new(snowflakeGcsClient).nativeDownloadFile(&meta, "dummy data", 1)
	if err == nil {
		t.Error("should have raised an error")
	}
}

func TestGetGcsCustomEndpoint(t *testing.T) {
	testcases := []struct {
		desc string
		in   execResponseStageInfo
		out  string
	}{
		{
			desc: "when the endPoint is not specified and UseRegionalURL is false",
			in: execResponseStageInfo{
				UseRegionalURL: false,
				EndPoint:       "",
				Region:         "WEST-1",
			},
			out: "https://storage.googleapis.com",
		},
		{
			desc: "when the useRegionalURL is only enabled",
			in: execResponseStageInfo{
				UseRegionalURL: true,
				EndPoint:       "",
				Region:         "mockLocation",
			},
			out: "https://storage.mocklocation.rep.googleapis.com",
		},
		{
			desc: "when the region is me-central2",
			in: execResponseStageInfo{
				UseRegionalURL: false,
				EndPoint:       "",
				Region:         "me-central2",
			},
			out: "https://storage.me-central2.rep.googleapis.com",
		},
		{
			desc: "when the region is me-central2 (mixed case)",
			in: execResponseStageInfo{
				UseRegionalURL: false,
				EndPoint:       "",
				Region:         "ME-cEntRal2",
			},
			out: "https://storage.me-central2.rep.googleapis.com",
		},
		{
			desc: "when the region is me-central2 (uppercase)",
			in: execResponseStageInfo{
				UseRegionalURL: false,
				EndPoint:       "",
				Region:         "ME-CENTRAL2",
			},
			out: "https://storage.me-central2.rep.googleapis.com",
		},
		{
			desc: "when the endPoint is specified",
			in: execResponseStageInfo{
				UseRegionalURL: false,
				EndPoint:       "storage.specialEndPoint.rep.googleapis.com",
				Region:         "ME-cEntRal1",
			},
			out: "https://storage.specialEndPoint.rep.googleapis.com",
		},
		{
			desc: "when both the endPoint and the useRegionalUrl are specified",
			in: execResponseStageInfo{
				UseRegionalURL: true,
				EndPoint:       "storage.specialEndPoint.rep.googleapis.com",
				Region:         "ME-cEntRal1",
			},
			out: "https://storage.specialEndPoint.rep.googleapis.com",
		},
		{
			desc: "when both the endPoint is specified and the region is me-central2",
			in: execResponseStageInfo{
				UseRegionalURL: true,
				EndPoint:       "storage.specialEndPoint.rep.googleapis.com",
				Region:         "ME-CENTRAL2",
			},
			out: "https://storage.specialEndPoint.rep.googleapis.com",
		},
	}

	for _, test := range testcases {
		t.Run(test.desc, func(t *testing.T) {
			endpoint := getGcsCustomEndpoint(&test.in)
			if endpoint != test.out {
				t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.out, endpoint)
			}
		})
	}
}
