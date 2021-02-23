// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

//lint:file-ignore U1000 Ignore all unused code

// implemented by localUtil and cloudUtil
type storageUtil interface {
	createClient(execResponseStageInfo, bool) cloudClient
	uploadOneFileWithRetry(*fileMetadata)
	downloadOneFile()
}

// implemented by snowflakeS3Util, snowflakeAzureUtil and snowflakeGcsUtil
type cloudUtil interface {
	getFileHeader()
	uploadFile()
	nativeDownloadFile()
}

type cloudClient interface{}
