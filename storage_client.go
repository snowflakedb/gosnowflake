// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

//lint:file-ignore U1000 TODO SNOW-29352

// implemented by localUtil and cloudUtil
type storageUtil interface {
	createClient(*execResponseStageInfo, bool) cloudClient
	uploadOneFileWithRetry(*fileMetadata) error
	downloadOneFile()
}

// implemented by snowflakeS3Util, snowflakeAzureUtil and snowflakeGcsUtil
type cloudUtil interface {
	getFileHeader(*fileMetadata, string) *fileHeader
	uploadFile(string, *fileMetadata, *encryptMetadata, int64)
	nativeDownloadFile()
}

type cloudClient interface{}
