// Copyright (c) 2023 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"
)

func TestLocalUpload(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "local_put")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)
	fname := filepath.Join(tmpDir, "test_put_get.txt.gz")
	originalContents := "123,test1\n456,test2\n"

	var b bytes.Buffer
	gzw := gzip.NewWriter(&b)
	_, err = gzw.Write([]byte(originalContents))
	assertNilF(t, err)
	assertNilF(t, gzw.Close())
	if err := os.WriteFile(fname, b.Bytes(), readWriteFileMode); err != nil {
		t.Fatal("could not write to gzip file")
	}
	putDir, err := os.MkdirTemp("", "put")
	if err != nil {
		t.Error(err)
	}

	info := execResponseStageInfo{
		Location:     putDir,
		LocationType: "LOCAL_FS",
	}
	localUtil := new(localUtil)
	localCli, err := localUtil.createClient(&info, false, nil)
	if err != nil {
		t.Error(err)
	}
	uploadMeta := fileMetadata{
		name:              "data1.txt.gz",
		stageLocationType: "LOCAL_FS",
		noSleepingTime:    true,
		parallel:          4,
		client:            localCli,
		stageInfo:         &info,
		dstFileName:       "data1.txt.gz",
		srcFileName:       path.Join(tmpDir, "/test_put_get.txt.gz"),
		overwrite:         true,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
	}
	uploadMeta.realSrcFileName = uploadMeta.srcFileName
	err = localUtil.uploadOneFileWithRetry(&uploadMeta)
	if err != nil {
		t.Error(err)
	}
	if uploadMeta.resStatus != uploaded {
		t.Fatalf("failed to upload file")
	}

	uploadMeta.overwrite = false
	err = localUtil.uploadOneFileWithRetry(&uploadMeta)
	if err != nil {
		t.Error(err)
	}
	if uploadMeta.resStatus != skipped {
		t.Fatal("overwrite is false. should have skipped")
	}
	fileStream, _ := os.Open(fname)
	ctx := WithFileStream(context.Background(), fileStream)
	uploadMeta.srcStream, err = getFileStream(ctx)
	assertNilF(t, err)

	err = localUtil.uploadOneFileWithRetry(&uploadMeta)
	if err != nil {
		t.Error(err)
	}
	if uploadMeta.resStatus != skipped {
		t.Fatalf("overwrite is false. should have skipped")
	}
	uploadMeta.overwrite = true
	err = localUtil.uploadOneFileWithRetry(&uploadMeta)
	if err != nil {
		t.Error(err)
	}
	if uploadMeta.resStatus != uploaded {
		t.Fatalf("failed to upload file")
	}

	uploadMeta.realSrcStream = uploadMeta.srcStream
	err = localUtil.uploadOneFileWithRetry(&uploadMeta)
	if err != nil {
		t.Error(err)
	}
	if uploadMeta.resStatus != uploaded {
		t.Fatalf("failed to upload file")
	}
}

func TestDownloadLocalFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "local_put")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		assertNilF(t, os.RemoveAll(tmpDir))
	}()
	fname := filepath.Join(tmpDir, "test_put_get.txt.gz")
	originalContents := "123,test1\n456,test2\n"

	var b bytes.Buffer
	gzw := gzip.NewWriter(&b)
	_, err = gzw.Write([]byte(originalContents))
	assertNilF(t, err)
	assertNilF(t, gzw.Close())
	if err := os.WriteFile(fname, b.Bytes(), readWriteFileMode); err != nil {
		t.Fatal("could not write to gzip file")
	}
	putDir, err := os.MkdirTemp("", "put")
	if err != nil {
		t.Error(err)
	}

	info := execResponseStageInfo{
		Location:     tmpDir,
		LocationType: "LOCAL_FS",
	}
	localUtil := new(localUtil)
	localCli, err := localUtil.createClient(&info, false, nil)
	if err != nil {
		t.Error(err)
	}
	downloadMeta := fileMetadata{
		name:              "test_put_get.txt.gz",
		stageLocationType: "LOCAL_FS",
		noSleepingTime:    true,
		client:            localCli,
		stageInfo:         &info,
		dstFileName:       "test_put_get.txt.gz",
		overwrite:         true,
		srcFileName:       "test_put_get.txt.gz",
		localLocation:     putDir,
		options: &SnowflakeFileTransferOptions{
			MultiPartThreshold: dataSizeThreshold,
		},
	}
	err = localUtil.downloadOneFile(&downloadMeta)
	if err != nil {
		t.Error(err)
	}
	if downloadMeta.resStatus != downloaded {
		t.Fatalf("failed to get file in local storage")
	}

	downloadMeta.srcFileName = "test_put_get.txt.gz"
	err = localUtil.downloadOneFile(&downloadMeta)
	if err != nil {
		t.Error(err)
	}
	if downloadMeta.resStatus != downloaded {
		t.Fatalf("failed to get file in local storage")
	}

	downloadMeta.srcFileName = "local://test_put_get.txt.gz"
	err = localUtil.downloadOneFile(&downloadMeta)
	if err == nil {
		t.Error("file name is invalid. should have returned an error")
	}
}
