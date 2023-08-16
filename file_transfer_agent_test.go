// Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
)

type tcFilePath struct {
	command string
	path    string
}

func TestGetBucketAccelerateConfiguration(t *testing.T) {
	if runningOnGithubAction() {
		t.Skip("Should be run against an account in AWS EU North1 region.")
	}
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}
	sfa := &snowflakeFileTransferAgent{
		sc:          sc,
		commandType: uploadCommand,
		srcFiles:    make([]string, 0),
		data: &execResponseData{
			SrcLocations: make([]string, 0),
		},
	}
	if err = sfa.transferAccelerateConfig(); err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			if ae.ErrorCode() == "MethodNotAllowed" {
				t.Fatalf("should have ignored 405 error: %v", err)
			}
		}
	}
}

func TestUnitDownloadWithInvalidLocalPath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "data")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)
	testData := filepath.Join(tmpDir, "data.txt")
	f, err := os.Create(testData)
	if err != nil {
		t.Error(err)
	}
	f.WriteString("test1,test2\ntest3,test4\n")
	f.Close()

	runDBTest(t, func(dbt *DBTest) {
		if _, err = dbt.exec("use role sysadmin"); err != nil {
			t.Skip("snowflake admin account not accessible")
		}
		dbt.mustExec("rm @~/test_get")
		sqlText := fmt.Sprintf("put file://%v @~/test_get", testData)
		sqlText = strings.ReplaceAll(sqlText, "\\", "\\\\")
		dbt.mustExec(sqlText)

		sqlText = fmt.Sprintf("get @~/test_get/data.txt file://%v\\get", tmpDir)
		if _, err = dbt.query(sqlText); err == nil {
			t.Fatalf("should return local path not directory error.")
		}
		dbt.mustExec("rm @~/test_get")
	})
}
func TestUnitGetLocalFilePathFromCommand(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}
	sfa := &snowflakeFileTransferAgent{
		sc:          sc,
		commandType: uploadCommand,
		srcFiles:    make([]string, 0),
		data: &execResponseData{
			SrcLocations: make([]string, 0),
		},
	}
	testcases := []tcFilePath{
		{"PUT file:///tmp/my_data_file.txt @~ overwrite=true", "/tmp/my_data_file.txt"},
		{"PUT 'file:///tmp/my_data_file.txt' @~ overwrite=true", "/tmp/my_data_file.txt"},
		{"PUT file:///tmp/sub_dir/my_data_file.txt\n @~ overwrite=true", "/tmp/sub_dir/my_data_file.txt"},
		{"PUT file:///tmp/my_data_file.txt    @~ overwrite=true", "/tmp/my_data_file.txt"},
		{"", ""},
		{"PUT 'file2:///tmp/my_data_file.txt' @~ overwrite=true", ""},
	}
	for _, test := range testcases {
		t.Run(test.command, func(t *testing.T) {
			path := sfa.getLocalFilePathFromCommand(test.command)
			if path != test.path {
				t.Fatalf("unexpected file path. expected: %v, but got: %v", test.path, path)
			}
		})
	}
}

func TestUnitProcessFileCompressionType(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}

	sfa := &snowflakeFileTransferAgent{
		sc:          sc,
		commandType: uploadCommand,
		srcFiles:    make([]string, 0),
	}
	testcases := []struct {
		srcCompression string
	}{
		{"none"},
		{"auto_detect"},
		{"gzip"},
	}

	for _, test := range testcases {
		t.Run(test.srcCompression, func(t *testing.T) {
			sfa.srcCompression = test.srcCompression
			err = sfa.processFileCompressionType()
			if err != nil {
				t.Fatalf("failed to process file compression")
			}
		})
	}

	// test invalid compression type error
	sfa.srcCompression = "gz"
	data := &execResponseData{
		SQLState: "S00087",
		QueryID:  "01aa2e8b-0405-ab7c-0000-53b10632f626",
	}
	sfa.data = data
	err = sfa.processFileCompressionType()
	if err == nil {
		t.Fatal("should have failed")
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("should be snowflake error. err: %v", err)
	}
	if driverErr.Number != ErrCompressionNotSupported {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrCompressionNotSupported, driverErr.Number)
	}
}

func TestParseCommandWithInvalidStageLocation(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}
	sfa := &snowflakeFileTransferAgent{
		sc:          sc,
		commandType: uploadCommand,
		srcFiles:    make([]string, 0),
		data: &execResponseData{
			SrcLocations: make([]string, 0),
		},
	}

	err = sfa.parseCommand()
	if err == nil {
		t.Fatal("should have raised an error")
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrInvalidStageLocation {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrInvalidStageLocation, driverErr.Number)
	}
}

func TestParseCommandEncryptionMaterialMismatchError(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}

	mockEncMaterial1 := snowflakeFileEncryption{
		QueryStageMasterKey: "abCdEFO0upIT36dAxGsa0w==",
		QueryID:             "01abc874-0406-1bf0-0000-53b10668e056",
		SMKID:               92019681909886,
	}

	mockEncMaterial2 := snowflakeFileEncryption{
		QueryStageMasterKey: "abCdEFO0upIT36dAxGsa0w==",
		QueryID:             "01abc874-0406-1bf0-0000-53b10668e056",
		SMKID:               92019681909886,
	}

	sfa := &snowflakeFileTransferAgent{
		sc:          sc,
		commandType: uploadCommand,
		srcFiles:    make([]string, 0),
		data: &execResponseData{
			SrcLocations: []string{"/tmp/uploads"},
			EncryptionMaterial: encryptionWrapper{
				snowflakeFileEncryption: mockEncMaterial1,
				EncryptionMaterials:     []snowflakeFileEncryption{mockEncMaterial1, mockEncMaterial2},
			},
		},
	}

	err = sfa.parseCommand()
	if err == nil {
		t.Fatal("should have raised an error")
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrInternalNotMatchEncryptMaterial {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrInternalNotMatchEncryptMaterial, driverErr.Number)
	}
}

func TestParseCommandInvalidStorageClientException(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}

	tmpDir, err := os.MkdirTemp("", "abc")
	if err != nil {
		t.Error(err)
	}
	mockEncMaterial1 := snowflakeFileEncryption{
		QueryStageMasterKey: "abCdEFO0upIT36dAxGsa0w==",
		QueryID:             "01abc874-0406-1bf0-0000-53b10668e056",
		SMKID:               92019681909886,
	}

	sfa := &snowflakeFileTransferAgent{
		sc:          sc,
		commandType: uploadCommand,
		srcFiles:    make([]string, 0),
		data: &execResponseData{
			SrcLocations:  []string{"/tmp/uploads"},
			LocalLocation: tmpDir,
			EncryptionMaterial: encryptionWrapper{
				snowflakeFileEncryption: mockEncMaterial1,
				EncryptionMaterials:     []snowflakeFileEncryption{mockEncMaterial1},
			},
		},
		options: &SnowflakeFileTransferOptions{
			DisablePutOverwrite: false,
		},
	}

	err = sfa.parseCommand()
	if err == nil {
		t.Fatal("should have raised an error")
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrInvalidStageFs {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrInvalidStageFs, driverErr.Number)
	}
}

func TestInitFileMetadataError(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}

	sfa := &snowflakeFileTransferAgent{
		sc:          sc,
		commandType: uploadCommand,
		srcFiles:    []string{"fileDoesNotExist.txt"},
		data: &execResponseData{
			SQLState: "123456",
			QueryID:  "01aa2e8b-0405-ab7c-0000-53b10632f626",
		},
	}

	err = sfa.initFileMetadata()
	if err == nil {
		t.Fatal("should have raised an error")
	}

	driverErr, ok := err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrFileNotExists {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrFileNotExists, driverErr.Number)
	}

	tmpDir, err := os.MkdirTemp("", "data")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)
	sfa.srcFiles = []string{tmpDir}

	err = sfa.initFileMetadata()
	if err == nil {
		t.Fatal("should have raised an error")
	}

	driverErr, ok = err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrFileNotExists {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrFileNotExists, driverErr.Number)
	}
}

func TestUpdateMetadataWithPresignedUrl(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}
	info := execResponseStageInfo{
		Location:     "gcs-blob/storage/users/456/",
		LocationType: "GCS",
	}

	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	testURL := "https://storage.google.com/gcs-blob/storage/users/456?Signature=testsignature123"

	presignedURLMock := func(_ context.Context, _ *snowflakeRestful,
		_ *url.Values, _ map[string]string, _ []byte, _ time.Duration,
		requestID UUID, _ *Config) (*execResponse, error) {
		// ensure the same requestID from context is used
		if len(requestID) == 0 {
			t.Fatal("requestID is empty")
		}
		dd := &execResponseData{
			QueryID: "01aa2e8b-0405-ab7c-0000-53b10632f626",
			Command: string(uploadCommand),
			StageInfo: execResponseStageInfo{
				LocationType: "GCS",
				Location:     "gcspuscentral1-4506459564-stage/users/456",
				Path:         "users/456",
				Region:       "US_CENTRAL1",
				PresignedURL: testURL,
			},
		}
		return &execResponse{
			Data:    *dd,
			Message: "",
			Code:    "0",
			Success: true,
		}, nil
	}

	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "GCS",
		noSleepingTime:    true,
		client:            gcsCli,
		sha256Digest:      "123456789abcdef",
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		srcFileName:       path.Join(dir, "/test_data/data1.txt"),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
	}

	sc.rest.FuncPostQuery = presignedURLMock
	sfa := &snowflakeFileTransferAgent{
		sc:                sc,
		commandType:       uploadCommand,
		command:           "put file:///tmp/test_data/data1.txt @~",
		stageLocationType: gcsClient,
		fileMetadata:      []*fileMetadata{&uploadMeta},
	}

	err = sfa.updateFileMetadataWithPresignedURL()
	if err != nil {
		t.Error(err)
	}
	if testURL != sfa.fileMetadata[0].presignedURL.String() {
		t.Fatalf("failed to update metadata with presigned url. expected: %v. got: %v", testURL, sfa.fileMetadata[0].presignedURL.String())
	}
}

func TestUpdateMetadataWithPresignedUrlForDownload(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}
	info := execResponseStageInfo{
		Location:     "gcs-blob/storage/users/456/",
		LocationType: "GCS",
	}

	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	testURL := "https://storage.google.com/gcs-blob/storage/users/456?Signature=testsignature123"

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
	}

	sfa := &snowflakeFileTransferAgent{
		sc:                sc,
		commandType:       downloadCommand,
		command:           "get @~/data1.txt.gz file:///tmp/testData",
		stageLocationType: gcsClient,
		fileMetadata:      []*fileMetadata{&downloadMeta},
		presignedURLs:     []string{testURL},
	}

	err = sfa.updateFileMetadataWithPresignedURL()
	if err != nil {
		t.Error(err)
	}
	if testURL != sfa.fileMetadata[0].presignedURL.String() {
		t.Fatalf("failed to update metadata with presigned url. expected: %v. got: %v", testURL, sfa.fileMetadata[0].presignedURL.String())
	}
}

func TestUpdateMetadataWithPresignedUrlError(t *testing.T) {
	config, err := ParseDSN(dsn)
	if err != nil {
		t.Error(err)
	}
	sc, err := buildSnowflakeConn(context.Background(), *config)
	if err != nil {
		t.Fatal(err)
	}
	if err = authenticateWithConfig(sc); err != nil {
		t.Fatal(err)
	}

	sfa := &snowflakeFileTransferAgent{
		sc:                sc,
		command:           "get @~/data1.txt.gz file:///tmp/testData",
		stageLocationType: gcsClient,
		data: &execResponseData{
			SQLState: "123456",
			QueryID:  "01aa2e8b-0405-ab7c-0000-53b10632f626",
		},
	}

	err = sfa.updateFileMetadataWithPresignedURL()
	if err == nil {
		t.Fatal("should have raised an error")
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok || driverErr.Number != ErrCommandNotRecognized {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrCommandNotRecognized, driverErr.Number)
	}
}

func TestUploadWhenFilesystemReadOnlyError(t *testing.T) {
	if isWindows {
		t.Skip("permission model is different")
	}

	var err error
	roPath := t.TempDir()
	if err != nil {
		t.Fatal(err)
	}

	// Set the temp directory to read only
	err = os.Chmod(roPath, 0444)
	if err != nil {
		t.Fatal(err)
	}

	info := execResponseStageInfo{
		Location:     "gcs-blob/storage/users/456/",
		LocationType: "GCS",
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Error(err)
	}

	// Make sure that the test uses read only directory
	t.Setenv("TMPDIR", roPath)

	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "GCS",
		noSleepingTime:    true,
		client:            gcsClient,
		sha256Digest:      "123456789abcdef",
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		srcFileName:       path.Join(dir, "/test_data/data1.txt"),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
	}

	sfa := &snowflakeFileTransferAgent{
		sc: &snowflakeConn{
			cfg: &Config{},
		},
		commandType:       uploadCommand,
		command:           "put file:///tmp/test_data/data1.txt @~",
		stageLocationType: gcsClient,
		fileMetadata:      []*fileMetadata{&uploadMeta},
		parallel:          1,
	}

	err = sfa.uploadFilesParallel([]*fileMetadata{&uploadMeta})
	if err == nil {
		t.Fatal("should error when the filesystem is read only")
	}
	if !strings.Contains(err.Error(), "errors during file upload:\nmkdir") {
		t.Fatalf("should error when creating the temporary directory. Instead errored with: %v", err)
	}
}

func TestUnitUpdateProgess(t *testing.T) {
	var b bytes.Buffer
	buf := io.Writer(&b)
	buf.Write([]byte("testing"))

	spp := &snowflakeProgressPercentage{
		filename:        "test.txt",
		fileSize:        float64(1500),
		outputStream:    &buf,
		showProgressBar: true,
		done:            false,
	}

	spp.call(0)
	if spp.done != false {
		t.Fatal("should not be done.")
	}

	if spp.seenSoFar != 0 {
		t.Fatalf("expected seenSoFar to be 0 but was %v", spp.seenSoFar)
	}

	spp.call(1516)
	if spp.done != true {
		t.Fatal("should be done after updating progess")
	}
}

func TestCustomTmpDirPath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("cannot create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	uploadFile := filepath.Join(tmpDir, "data.txt")
	f, err := os.Create(uploadFile)
	if err != nil {
		t.Error(err)
	}
	f.WriteString("test1,test2\ntest3,test4\n")
	f.Close()

	uploadMeta := &fileMetadata{
		name:              "data.txt.gz",
		stageLocationType: "local",
		noSleepingTime:    true,
		client:            local,
		sha256Digest:      "123456789abcdef",
		stageInfo: &execResponseStageInfo{
			Location:     tmpDir,
			LocationType: "local",
		},
		dstFileName: "data.txt.gz",
		srcFileName: uploadFile,
		overwrite:   true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
	}

	downloadFile := filepath.Join(tmpDir, "download.txt")
	downloadMeta := &fileMetadata{
		name:              "data.txt.gz",
		stageLocationType: "local",
		noSleepingTime:    true,
		client:            local,
		sha256Digest:      "123456789abcdef",
		stageInfo: &execResponseStageInfo{
			Location:     tmpDir,
			LocationType: "local",
		},
		srcFileName: "data.txt.gz",
		dstFileName: downloadFile,
		overwrite:   true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
	}

	sfa := snowflakeFileTransferAgent{
		sc: &snowflakeConn{
			cfg: &Config{
				TmpDirPath: tmpDir,
			},
		},
		stageLocationType: local,
	}
	_, err = sfa.uploadOneFile(uploadMeta)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sfa.downloadOneFile(downloadMeta)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("download.txt")
}

func TestReadonlyTmpDirPathShouldFail(t *testing.T) {
	if isWindows {
		t.Skip("permission model is different")
	}
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("cannot create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	uploadFile := filepath.Join(tmpDir, "data.txt")
	f, err := os.Create(uploadFile)
	if err != nil {
		t.Error(err)
	}
	f.WriteString("test1,test2\ntest3,test4\n")
	f.Close()

	err = os.Chmod(tmpDir, 0400)
	if err != nil {
		t.Fatalf("cannot mark directory as readonly: %v", err)
	}
	defer os.Chmod(tmpDir, 0600)

	uploadMeta := &fileMetadata{
		name:              "data.txt.gz",
		stageLocationType: "local",
		noSleepingTime:    true,
		client:            local,
		sha256Digest:      "123456789abcdef",
		stageInfo: &execResponseStageInfo{
			Location:     tmpDir,
			LocationType: "local",
		},
		dstFileName: "data.txt.gz",
		srcFileName: uploadFile,
		overwrite:   true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
	}

	sfa := snowflakeFileTransferAgent{
		sc: &snowflakeConn{
			cfg: &Config{
				TmpDirPath: tmpDir,
			},
		},
		stageLocationType: local,
	}
	_, err = sfa.uploadOneFile(uploadMeta)
	if err == nil {
		t.Fatalf("should not upload file as temporary directory is not readable")
	}
}
