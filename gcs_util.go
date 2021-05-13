package gosnowflake

type snowflakeGcsUtil struct {
}

// storageUtil implementation
func (util *snowflakeGcsUtil) createClient(info *execResponseStageInfo, useAccelerateEndpoint bool) cloudClient {
	securityToken := info.Creds.GcsAccessToken
	var client cloudClient
	if securityToken != "" {
		client = securityToken
	} else {
		client = nil
	}
	return client
}

// storageUtil implementation
func (util *snowflakeGcsUtil) uploadOneFileWithRetry(meta *fileMetadata) error {
	// TODO SNOW-294155
	return nil
}

// storageUtil implementation
func (util *snowflakeGcsUtil) downloadOneFile() {
	// TODO SNOW-294151
}

// cloudUtil implementation
func (util *snowflakeGcsUtil) getFileHeader(meta *fileMetadata, filename string) *fileHeader {
	// TODO SNOW-294155
	return &fileHeader{}
}

// cloudUtil implementation
func (util *snowflakeGcsUtil) uploadFile(dataFile string, meta *fileMetadata, encryptMeta *encryptMetadata, maxConcurrency int, multiPartThreshold int64) error {
	// TODO SNOW-294155
	return nil
}

// cloudUtil implementation
func (util *snowflakeGcsUtil) nativeDownloadFile() {
	// TODO SNOW-294151
}
