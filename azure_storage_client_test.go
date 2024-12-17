// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
)

func TestExtractContainerNameAndPath(t *testing.T) {
	azureUtil := new(snowflakeAzureClient)
	testcases := []tcBucketPath{
		{"sfc-eng-regression/test_sub_dir/", "sfc-eng-regression", "test_sub_dir/"},
		{"sfc-eng-regression/dir/test_stg/test_sub_dir/", "sfc-eng-regression", "dir/test_stg/test_sub_dir/"},
		{"sfc-eng-regression/", "sfc-eng-regression", ""},
		{"sfc-eng-regression//", "sfc-eng-regression", "/"},
		{"sfc-eng-regression///", "sfc-eng-regression", "//"},
	}
	for _, test := range testcases {
		t.Run(test.in, func(t *testing.T) {
			azureLoc, err := azureUtil.extractContainerNameAndPath(test.in)
			if err != nil {
				t.Error(err)
			}
			if azureLoc.containerName != test.bucket {
				t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.bucket, azureLoc.containerName)
			}
			if azureLoc.path != test.path {
				t.Errorf("failed. in: %v, expected: %v, got: %v", test.in, test.path, azureLoc.path)
			}
		})
	}
}

func TestUnitDetectAzureTokenExpireError(t *testing.T) {
	azureUtil := new(snowflakeAzureClient)
	dd := &execResponseData{}
	invalidSig := &execResponse{
		Data:    *dd,
		Message: "Signature not valid in the specified time frame",
		Code:    "403",
		Success: true,
	}
	ba, err := json.Marshal(invalidSig)
	if err != nil {
		panic(err)
	}
	resp := &http.Response{StatusCode: http.StatusForbidden, Body: &fakeResponseBody{body: ba}}
	if !azureUtil.detectAzureTokenExpireError(resp) {
		t.Fatal("expected token expired")
	}

	invalidAuth := &execResponse{
		Data:    *dd,
		Message: "Server failed to authenticate the request",
		Code:    "403",
		Success: true,
	}
	ba, err = json.Marshal(invalidAuth)
	if err != nil {
		panic(err)
	}
	resp = &http.Response{StatusCode: http.StatusForbidden, Body: &fakeResponseBody{body: ba}}
	if !azureUtil.detectAzureTokenExpireError(resp) {
		t.Fatal("expected token expired")
	}

	resp = &http.Response{
		StatusCode: http.StatusForbidden,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}},
	}
	if azureUtil.detectAzureTokenExpireError(resp) {
		t.Fatal("invalid body")
	}

	invalidMessage := &execResponse{
		Data:    *dd,
		Message: "unauthorized",
		Code:    "403",
		Success: true,
	}
	ba, err = json.Marshal(invalidMessage)
	if err != nil {
		panic(err)
	}
	resp = &http.Response{StatusCode: http.StatusForbidden, Body: &fakeResponseBody{body: ba}}
	if azureUtil.detectAzureTokenExpireError(resp) {
		t.Fatal("incorrect message")
	}

	resp = &http.Response{
		StatusCode: http.StatusOK,
		Body:       &fakeResponseBody{body: []byte{0x12, 0x34}}}

	if azureUtil.detectAzureTokenExpireError(resp) {
		t.Fatal("status code is success. expected false.")
	}
}

type azureObjectAPIMock struct {
	UploadStreamFunc   func(ctx context.Context, body io.Reader, o *azblob.UploadStreamOptions) (azblob.UploadStreamResponse, error)
	UploadFileFunc     func(ctx context.Context, file *os.File, o *azblob.UploadFileOptions) (azblob.UploadFileResponse, error)
	DownloadFileFunc   func(ctx context.Context, file *os.File, o *blob.DownloadFileOptions) (int64, error)
	DownloadStreamFunc func(ctx context.Context, o *blob.DownloadStreamOptions) (azblob.DownloadStreamResponse, error)
	GetPropertiesFunc  func(ctx context.Context, o *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error)
}

func (c *azureObjectAPIMock) UploadStream(ctx context.Context, body io.Reader, o *azblob.UploadStreamOptions) (azblob.UploadStreamResponse, error) {
	return c.UploadStreamFunc(ctx, body, o)
}

func (c *azureObjectAPIMock) UploadFile(ctx context.Context, file *os.File, o *azblob.UploadFileOptions) (azblob.UploadFileResponse, error) {
	return c.UploadFileFunc(ctx, file, o)
}

func (c *azureObjectAPIMock) GetProperties(ctx context.Context, o *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
	return c.GetPropertiesFunc(ctx, o)
}

func (c *azureObjectAPIMock) DownloadFile(ctx context.Context, file *os.File, o *blob.DownloadFileOptions) (int64, error) {
	return c.DownloadFileFunc(ctx, file, o)
}

func (c *azureObjectAPIMock) DownloadStream(ctx context.Context, o *blob.DownloadStreamOptions) (azblob.DownloadStreamResponse, error) {
	return c.DownloadStreamFunc(ctx, o)
}

func TestUploadFileWithAzureUploadFailedError(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "azblob/storage/users/456/",
		LocationType: "AZURE",
	}
	initialParallel := int64(100)
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}
	encMat := snowflakeFileEncryption{
		QueryStageMasterKey: "abCdEFO0upIT36dAxGsa0w==",
		QueryID:             "01abc874-0406-1bf0-0000-53b10668e056",
		SMKID:               92019681909886,
	}

	azureCli, err := new(snowflakeAzureClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:               "data1.txt.gz",
		stageLocationType:  "AZURE",
		noSleepingTime:     true,
		parallel:           initialParallel,
		client:             azureCli,
		sha256Digest:       "123456789abcdef",
		stageInfo:          &info,
		dstFileName:        "data1.txt.gz",
		srcFileName:        path.Join(dir, "/test_data/put_get_1.txt"),
		encryptionMaterial: &encMat,
		overwrite:          true,
		dstCompressionType: compressionTypes["GZIP"],
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockAzureClient: &azureObjectAPIMock{
			UploadFileFunc: func(ctx context.Context, file *os.File, o *azblob.UploadFileOptions) (azblob.UploadFileResponse, error) {
				return azblob.UploadFileResponse{}, errors.New("unexpected error uploading file")
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

func TestUploadStreamWithAzureUploadFailedError(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "azblob/storage/users/456/",
		LocationType: "AZURE",
	}
	initialParallel := int64(100)
	src := []byte{65, 66, 67}
	encMat := snowflakeFileEncryption{
		QueryStageMasterKey: "abCdEFO0upIT36dAxGsa0w==",
		QueryID:             "01abc874-0406-1bf0-0000-53b10668e056",
		SMKID:               92019681909886,
	}

	azureCli, err := new(snowflakeAzureClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:               "data1.txt.gz",
		stageLocationType:  "AZURE",
		noSleepingTime:     true,
		parallel:           initialParallel,
		client:             azureCli,
		sha256Digest:       "123456789abcdef",
		stageInfo:          &info,
		dstFileName:        "data1.txt.gz",
		srcStream:          bytes.NewBuffer(src),
		encryptionMaterial: &encMat,
		overwrite:          true,
		dstCompressionType: compressionTypes["GZIP"],
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockAzureClient: &azureObjectAPIMock{
			UploadStreamFunc: func(ctx context.Context, body io.Reader, o *azblob.UploadStreamOptions) (azblob.UploadStreamResponse, error) {
				return azblob.UploadStreamResponse{}, errors.New("unexpected error uploading file")
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

func TestUploadFileWithAzureUploadTokenExpired(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "azblob/storage/users/456/",
		LocationType: "AZURE",
	}
	initialParallel := int64(100)
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	dd := &execResponseData{}
	invalidSig := &execResponse{
		Data:    *dd,
		Message: "Signature not valid in the specified time frame",
		Code:    "403",
		Success: true,
	}
	ba, err := json.Marshal(invalidSig)
	if err != nil {
		panic(err)
	}

	azureCli, err := new(snowflakeAzureClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:               "data1.txt.gz",
		stageLocationType:  "AZURE",
		noSleepingTime:     true,
		parallel:           initialParallel,
		client:             azureCli,
		sha256Digest:       "123456789abcdef",
		stageInfo:          &info,
		dstFileName:        "data1.txt.gz",
		srcFileName:        path.Join(dir, "/test_data/put_get_1.txt"),
		overwrite:          true,
		dstCompressionType: compressionTypes["GZIP"],
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockAzureClient: &azureObjectAPIMock{
			UploadFileFunc: func(ctx context.Context, file *os.File, o *azblob.UploadFileOptions) (azblob.UploadFileResponse, error) {
				return azblob.UploadFileResponse{}, &azcore.ResponseError{
					ErrorCode:   "12345",
					StatusCode:  403,
					RawResponse: &http.Response{StatusCode: http.StatusForbidden, Body: &fakeResponseBody{body: ba}},
				}
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
		t.Fatal(err)
	}

	if uploadMeta.resStatus != renewToken {
		t.Fatalf("expected %v result status, got: %v",
			renewToken, uploadMeta.resStatus)
	}
}

func TestUploadFileWithAzureUploadNeedsRetry(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "azblob/storage/users/456/",
		LocationType: "AZURE",
	}
	initialParallel := int64(100)
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	dd := &execResponseData{}
	invalidSig := &execResponse{
		Data:    *dd,
		Message: "Server Error",
		Code:    "500",
		Success: true,
	}
	ba, err := json.Marshal(invalidSig)
	if err != nil {
		panic(err)
	}

	azureCli, err := new(snowflakeAzureClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:               "data1.txt.gz",
		stageLocationType:  "AZURE",
		noSleepingTime:     false,
		parallel:           initialParallel,
		client:             azureCli,
		sha256Digest:       "123456789abcdef",
		stageInfo:          &info,
		dstFileName:        "data1.txt.gz",
		srcFileName:        path.Join(dir, "/test_data/put_get_1.txt"),
		overwrite:          true,
		dstCompressionType: compressionTypes["GZIP"],
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockAzureClient: &azureObjectAPIMock{
			UploadFileFunc: func(ctx context.Context, file *os.File, o *azblob.UploadFileOptions) (azblob.UploadFileResponse, error) {
				return azblob.UploadFileResponse{}, &azcore.ResponseError{
					ErrorCode:   "12345",
					StatusCode:  500,
					RawResponse: &http.Response{StatusCode: http.StatusForbidden, Body: &fakeResponseBody{body: ba}},
				}
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
		t.Fatal("should have raised an error")
	}

	if uploadMeta.resStatus != needRetry {
		t.Fatalf("expected %v result status, got: %v",
			needRetry, uploadMeta.resStatus)
	}
}

func TestDownloadOneFileToAzureFailed(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "azblob/rwyitestacco/users/1234/",
		LocationType: "AZURE",
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	azureCli, err := new(snowflakeAzureClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	downloadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "AZURE",
		noSleepingTime:    true,
		client:            azureCli,
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		overwrite:         true,
		srcFileName:       "data1.txt.gz",
		localLocation:     dir,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
		mockAzureClient: &azureObjectAPIMock{
			DownloadFileFunc: func(ctx context.Context, file *os.File, o *blob.DownloadFileOptions) (int64, error) {
				return 0, errors.New("unexpected error uploading file")
			},
			GetPropertiesFunc: func(ctx context.Context, o *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
				return blob.GetPropertiesResponse{}, nil
			},
		},
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
}

func TestGetFileHeaderErrorStatus(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "azblob/teststage/users/34/",
		LocationType: "AZURE",
	}

	azureCli, err := new(snowflakeAzureClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	meta := fileMetadata{
		client:    azureCli,
		stageInfo: &info,
		mockAzureClient: &azureObjectAPIMock{
			GetPropertiesFunc: func(ctx context.Context, o *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
				return blob.GetPropertiesResponse{}, errors.New("failed to retrieve headers")
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}

	if header, err := (&snowflakeAzureClient{cfg: &Config{}}).getFileHeader(&meta, "file.txt"); header != nil || err == nil {
		t.Fatalf("expected null header, got: %v", header)
	}
	if meta.resStatus != errStatus {
		t.Fatalf("expected %v result status, got: %v", errStatus, meta.resStatus)
	}

	dd := &execResponseData{}
	invalidSig := &execResponse{
		Data:    *dd,
		Message: "Not Found",
		Code:    "404",
		Success: true,
	}
	ba, err := json.Marshal(invalidSig)
	if err != nil {
		panic(err)
	}

	meta = fileMetadata{
		client:    azureCli,
		stageInfo: &info,
		mockAzureClient: &azureObjectAPIMock{
			GetPropertiesFunc: func(ctx context.Context, o *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
				return blob.GetPropertiesResponse{}, &azcore.ResponseError{
					ErrorCode:   "BlobNotFound",
					StatusCode:  404,
					RawResponse: &http.Response{StatusCode: http.StatusNotFound, Body: &fakeResponseBody{body: ba}},
				}
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}

	if header, err := (&snowflakeAzureClient{cfg: &Config{}}).getFileHeader(&meta, "file.txt"); header != nil || err == nil {
		t.Fatalf("expected null header, got: %v", header)
	}
	if meta.resStatus != notFoundFile {
		t.Fatalf("expected %v result status, got: %v", errStatus, meta.resStatus)
	}

	invalidSig = &execResponse{
		Data:    *dd,
		Message: "Unauthorized",
		Code:    "403",
		Success: true,
	}
	ba, err = json.Marshal(invalidSig)
	if err != nil {
		panic(err)
	}
	meta.mockAzureClient = &azureObjectAPIMock{
		GetPropertiesFunc: func(ctx context.Context, o *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
			return blob.GetPropertiesResponse{}, &azcore.ResponseError{
				StatusCode:  403,
				RawResponse: &http.Response{StatusCode: http.StatusForbidden, Body: &fakeResponseBody{body: ba}},
			}
		},
	}

	if header, err := (&snowflakeAzureClient{cfg: &Config{}}).getFileHeader(&meta, "file.txt"); header != nil || err == nil {
		t.Fatalf("expected null header, got: %v", header)
	}
	if meta.resStatus != renewToken {
		t.Fatalf("expected %v result status, got: %v", renewToken, meta.resStatus)
	}
}

func TestUploadFileToAzureClientCastFail(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "azblob/rwyi-testacco/users/9220/",
		LocationType: "AZURE",
	}
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
		stageLocationType: "AZURE",
		noSleepingTime:    false,
		client:            s3Cli,
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

func TestAzureGetHeaderClientCastFail(t *testing.T) {
	info := execResponseStageInfo{
		Location:     "azblob/rwyi-testacco/users/9220/",
		LocationType: "AZURE",
	}
	s3Cli, err := new(snowflakeS3Client).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}

	meta := fileMetadata{
		client:    s3Cli,
		stageInfo: &execResponseStageInfo{Location: ""},
		mockAzureClient: &azureObjectAPIMock{
			GetPropertiesFunc: func(ctx context.Context, o *blob.GetPropertiesOptions) (blob.GetPropertiesResponse, error) {
				return blob.GetPropertiesResponse{}, nil
			},
		},
		sfa: &snowflakeFileTransferAgent{
			sc: &snowflakeConn{
				cfg: &Config{},
			},
		},
	}

	_, err = new(snowflakeAzureClient).getFileHeader(&meta, "file.txt")
	if err == nil {
		t.Fatal("should have failed")
	}
}
