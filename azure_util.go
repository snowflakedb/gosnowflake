// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

type snowflakeAzureUtil struct {
}

// storageUtil implementation
func (util *snowflakeAzureUtil) createClient(info *execResponseStageInfo, useAccelerateEndpoint bool) cloudClient {
	sasToken := info.Creds.AzureSasToken
	if sasToken != "" && strings.HasPrefix(sasToken, "?") {
		sasToken = sasToken[1:]
	}
	endPoint := info.EndPoint
	endPoint = strings.TrimPrefix(endPoint, "blob.")

	key, _ := azblob.NewSharedKeyCredential(info.StorageAccount, sasToken)
	p := azblob.NewPipeline(key, azblob.PipelineOptions{})
	u, _ := url.Parse("https://" + info.StorageAccount + ".blob." + endPoint)
	serviceURL := azblob.NewServiceURL(*u, p)
	containerURL := serviceURL.NewContainerURL("mycontainer")
	containerURL.Create(context.Background(), azblob.Metadata{}, azblob.PublicAccessNone)
	return containerURL
}

// storageUtil implementation
func (util *snowflakeAzureUtil) uploadOneFileWithRetry(meta *fileMetadata) error {
	// TODO SNOW-294155
	return nil
}

// storageUtil implementation
func (util *snowflakeAzureUtil) downloadOneFile() {
	// TODO SNOW-294151
}

// cloudUtil implementation
func (util *snowflakeAzureUtil) getFileHeader(meta *fileMetadata, filename string) *fileHeader {
	// TODO SNOW-294155
	return &fileHeader{}
}

// cloudUtil implementation
func (util *snowflakeAzureUtil) uploadFile(dataFile string, meta *fileMetadata, encryptMeta *encryptMetadata, maxConcurrency int, multiPartThreshold int64) error {
	// TODO SNOW-294155
	return nil
}

// cloudUtil implementation
func (util *snowflakeAzureUtil) nativeDownloadFile() {
	// TODO SNOW-294151
}
