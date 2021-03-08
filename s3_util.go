// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type snowflakeS3Util struct {
}

// storageUtil implementation
func (util *snowflakeS3Util) createClient(info *execResponseStageInfo, useAccelerateEndpoint bool) cloudClient {
	stageCredentials := info.Creds
	securityToken := stageCredentials.AwsToken
	var endPoint string
	if info.EndPoint != "" {
		endPoint = "https://" + info.EndPoint
	}

	return s3.New(s3.Options{
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
			stageCredentials.AwsKeyID,
			stageCredentials.AwsSecretKey,
			securityToken)),
		EndpointResolver: s3.EndpointResolverFromURL(endPoint),
		Region:           info.Region,
		UseAccelerate:    useAccelerateEndpoint,
	})
}

// storageUtil implementation
func (util *snowflakeS3Util) uploadOneFileWithRetry(meta *fileMetadata) error {
	// TODO SNOW-294154
	return nil
}

// storageUtil implementation
func (util *snowflakeS3Util) downloadOneFile() {
	// TODO SNOW-294151
}

// cloudUtil implementation
func (util *snowflakeS3Util) getFileHeader(meta *fileMetadata, filename string) *fileHeader {
	// TODO SNOW-294154
	return nil
}

// cloudUtil implementation
func (util *snowflakeS3Util) uploadFile(dataFile string, meta *fileMetadata, encryptMeta *encryptMetadata, maxConcurrency int64) {
	// TODO SNOW-294154
}

// cloudUtil implementation
func (util *snowflakeS3Util) nativeDownloadFile() {
	// TODO SNOW-294151
}
