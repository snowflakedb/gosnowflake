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
	return nil // TODO
}

// storageUtil implementation
func (util *snowflakeGcsUtil) downloadOneFile() {
	// TODO
}

// cloudUtil implementation
func (util *snowflakeGcsUtil) getFileHeader(meta *fileMetadata, filename string) *fileHeader {
	// TODO
	return &fileHeader{}
}

// cloudUtil implementation
func (util *snowflakeGcsUtil) uploadFile(dataFile string, meta *fileMetadata, encryptMeta *encryptMetadata, maxConcurrency int64) {
	// TODO
}

// cloudUtil implementation
func (util *snowflakeGcsUtil) nativeDownloadFile() {
	// TODO
}
