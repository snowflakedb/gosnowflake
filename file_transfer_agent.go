// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

//lint:file-ignore U1000 Ignore all unused code

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

type cloudType string
type commandType string

const (
	fileProtocol      = "file://"
	dataSizeThreshold = 67108864
	bigFileThreshold  = 200 * 1024 * 1024
	injectWaitPut     = 0
)

const (
	uploadCommand   commandType = "UPLOAD"
	downloadCommand commandType = "DOWNLOAD"
	unknownCommand  commandType = "UNKNOWN"
	putRegexp       string      = `(?i)^(?:/\*.*\*/\s*)*put\s+`
	getRegexp       string      = `(?i)^(?:/\*.*\*/\s*)*get\s+`
)

const (
	s3Client    cloudType = "S3"
	azureClient cloudType = "AZURE"
	gcsClient   cloudType = "GCS"
	local       cloudType = "LOCAL_FS"
)

type resultStatus int

const (
	err resultStatus = iota
	uploaded
	downloaded
	collision
	skipped
	renewToken
	renewPresignedURL
	notFoundFile
	needRetry
	needRetryWithLowerConcurrency
)

func (rs resultStatus) String() string {
	return [...]string{"ERROR", "UPLOADED", "DOWNLOADED", "COLLISION",
		"SKIPPED", "RENEW_TOKEN", "RENEW_PRESIGNED_URL", "NOT_FOUND_FILE",
		"NEED_RETRY", "NEED_RETRY_WITH_LOWER_CONCURRENCY"}[rs]
}

type snowflakeFileTransferAgent struct {
	data                        execResponseData
	command                     string
	commandType                 commandType
	stageLocationType           cloudType
	fileMetadata                []*fileMetadata
	encryptionMaterial          []*snowflakeFileEncryption
	stageInfo                   *execResponseStageInfo
	results                     []*fileMetadata
	srcLocations                []string
	autoCompress                bool
	srcCompression              string
	parallel                    int64
	overwrite                   bool
	srcFiles                    []string
	localLocation               string
	raisePutGetError            bool
	srcFileToEncryptionMaterial map[string]*snowflakeFileEncryption

	/* streaming PUT */
	sourceStream               *bytes.Buffer
	dstFileNameForStreamSource string
	compressSourceFromStream   bool

	/* PUT */
	forcePutOverwrite       bool
	putCallback             string
	putAzureCallback        string
	putCallbackOutputStream string

	/* GET */
	presignedURLs           []string
	getCallback             string
	getAzureCallback        string
	getCallbackOutputStream string
}

func (sfa *snowflakeFileTransferAgent) execute() error {
	fmt.Println("execute")
	var err error
	err = sfa.parseCommand()
	if err != nil {
		return err
	}
	err = sfa.initFileMetadata()
	if err != nil {
		return err
	}

	if sfa.commandType == uploadCommand {
		err = sfa.processFileCompressionType()
		if err != nil {
			return err
		}
	}

	if sfa.stageLocationType == local {
		if _, err := os.Stat(sfa.stageInfo.Location); os.IsNotExist(err) {
			err = os.MkdirAll(sfa.stageInfo.Location, os.ModePerm) // TODO what if not enough permissions?
			if err != nil {
				fmt.Println("")
				panic(err)
			}
		}
	}

	smallFileMetas := make([]*fileMetadata, 0)
	largeFileMetas := make([]*fileMetadata, 0)

	for _, meta := range sfa.fileMetadata {
		meta.overwrite = sfa.overwrite
		meta.sfa = sfa
		if sfa.stageLocationType != local {
			meta.putCallback = sfa.putCallback
			meta.putAzureCallback = sfa.putAzureCallback
			meta.putCallbackOutputStream = sfa.putCallbackOutputStream

			meta.getCallback = sfa.getCallback
			meta.getAzureCallback = sfa.getAzureCallback
			meta.getCallbackOutputStream = sfa.getCallbackOutputStream

			sizeThreshold := dataSizeThreshold
			if meta.srcFileSize > sizeThreshold {
				meta.parallel = sfa.parallel
				largeFileMetas = append(largeFileMetas, meta)
			} else {
				meta.parallel = 1
				smallFileMetas = append(smallFileMetas, meta)
			}
		} else {
			meta.parallel = 1
			smallFileMetas = append(smallFileMetas, meta)
		}
	}

	if sfa.commandType == uploadCommand {
		sfa.upload(largeFileMetas, smallFileMetas)
	}

	//if sfa.commandType == downloadCommand {
	//	// TODO SNOW-206124
	//}
	return nil
}

func (sfa *snowflakeFileTransferAgent) parseCommand() error {
	fmt.Println("parse command")
	if sfa.data.Command != "" {
		sfa.commandType = commandType(sfa.data.Command)
	} else {
		sfa.commandType = unknownCommand
	}

	sfa.initEncryptionMaterial()
	if len(sfa.data.SrcLocations) == 0 {
		return &SnowflakeError{
			Number:  ErrInvalidStageLocation,
			Message: "failed to parse location",
		}
	}
	sfa.srcLocations = sfa.data.SrcLocations

	if sfa.commandType == uploadCommand {
		if sfa.sourceStream != nil {
			sfa.srcFiles = sfa.srcLocations // streaming PUT
		}
		sfa.autoCompress = sfa.data.AutoCompress
		sfa.srcCompression = strings.ToLower(sfa.data.SourceCompression)
	} else {
		sfa.srcFiles = sfa.srcLocations
		sfa.srcFileToEncryptionMaterial = make(map[string]*snowflakeFileEncryption)
		if len(sfa.data.SrcLocations) == len(sfa.encryptionMaterial) {
			for i, srcFile := range sfa.srcFiles {
				sfa.srcFileToEncryptionMaterial[srcFile] = sfa.encryptionMaterial[i]
			}
		} else if len(sfa.encryptionMaterial) != 0 {
			return &SnowflakeError{
				Number: ErrInternalNotMatchEncryptMaterial,
				Message: fmt.Sprintf("number of downlodading files doesn't "+
					"match the encryption materials. files=%v, encmat=%v",
					len(sfa.data.SrcLocations), len(sfa.encryptionMaterial)),
			}
		}

		sfa.localLocation = expandUser(sfa.data.LocalLocation)
		fi, _ := os.Stat(sfa.localLocation)
		if !fi.IsDir() {
			return &SnowflakeError{
				Number: ErrLocalPathNotDirectory,
				Message: fmt.Sprintf("the local path is not a directory: %v",
					sfa.localLocation),
			}
		}
	}

	sfa.parallel = 1
	if sfa.data.Parallel != 0 {
		sfa.parallel = sfa.data.Parallel
	}
	sfa.overwrite = sfa.overwrite && sfa.forcePutOverwrite
	sfa.stageLocationType = cloudType(sfa.data.StageInfo.LocationType)
	sfa.stageInfo = &sfa.data.StageInfo
	sfa.presignedURLs = make([]string, 0)
	if len(sfa.data.PresignedURLs) != 0 {
		sfa.presignedURLs = sfa.data.PresignedURLs
	}

	if sfa.getStorageClient(sfa.stageLocationType) == nil {
		return &SnowflakeError{
			Number: ErrInvalidStageFs,
			Message: fmt.Sprintf("destination location type is not valid: %v",
				sfa.stageLocationType),
		}
	}
	return nil
}

func (sfa *snowflakeFileTransferAgent) initEncryptionMaterial() {
	sfa.encryptionMaterial = make([]*snowflakeFileEncryption, 0)
	wrapper := sfa.data.EncryptionMaterial

	if sfa.commandType == uploadCommand {
		sfa.encryptionMaterial = append(sfa.encryptionMaterial, &wrapper.snowflakeFileEncryption)
	} else {
		for _, encmat := range wrapper.EncryptionMaterials {
			sfa.encryptionMaterial = append(sfa.encryptionMaterial, &encmat)
		}
	}
}

func (sfa *snowflakeFileTransferAgent) initFileMetadata() error {
	fmt.Println("init file metadata")
	sfa.fileMetadata = make([]*fileMetadata, 0)
	if sfa.commandType == uploadCommand {
		if len(sfa.srcFiles) == 0 {
			fileName := sfa.data.SrcLocations
			return &SnowflakeError{
				Number: ErrFileNotExists,
				Message: fmt.Sprintf("file does not exist: %v",
					fileName),
			}
		}
		if sfa.sourceStream != nil {
			fileName := sfa.srcFiles[0]
			srcFileSize := sfa.sourceStream.Len()
			sfa.fileMetadata = append(sfa.fileMetadata, &fileMetadata{
				name:              baseName(fileName),
				srcFileName:       fileName,
				srcStream:         sfa.sourceStream,
				srcFileSize:       srcFileSize,
				stageLocationType: sfa.stageLocationType,
				stageInfo:         sfa.stageInfo,
			})
		}

		if len(sfa.encryptionMaterial) > 0 {
			for _, meta := range sfa.fileMetadata {
				meta.encryptionMaterial = sfa.encryptionMaterial[0]
			}
		}
	}
	return nil
}

func (sfa *snowflakeFileTransferAgent) processFileCompressionType() error {
	fmt.Println("process file compression type")
	var userSpecifiedSourceCompression *compressionType
	var autoDetect bool
	fct := new(fileCompressionType)
	fct.init()
	if sfa.srcCompression == "auto_detect" {
		autoDetect = true
	} else if sfa.srcCompression == "none" {
		autoDetect = false
	} else {
		userSpecifiedSourceCompression = fct.lookupByMimeSubType(sfa.srcCompression)
		if userSpecifiedSourceCompression == nil || !userSpecifiedSourceCompression.isSupported {
			return &SnowflakeError{
				Number: ErrCompressionNotSupported,
				Message: fmt.Sprintf("feature is not supported: %v",
					userSpecifiedSourceCompression),
			}
		}
		autoDetect = false
	}

	gzipCompression := compressionTypes["GZIP"]
	if sfa.sourceStream != nil {
		fileMeta := sfa.fileMetadata[0]
		fileMeta.srcCompressionType = userSpecifiedSourceCompression
		if sfa.compressSourceFromStream {
			fileMeta.dstCompressionType = &gzipCompression
			fileMeta.requireCompress = true
		} else {
			fileMeta.dstCompressionType = userSpecifiedSourceCompression
			fileMeta.requireCompress = false
		}

		// add gz extension if file name doesn't have it
		if sfa.compressSourceFromStream && strings.HasSuffix(sfa.dstFileNameForStreamSource, gzipCompression.fileExtension) {
			fileMeta.dstFileName = sfa.dstFileNameForStreamSource + gzipCompression.fileExtension
		} else {
			fileMeta.dstFileName = sfa.dstFileNameForStreamSource
		}
	} else {
		for _, meta := range sfa.fileMetadata {
			fileName := meta.srcFileName
			var currentFileCompressionType *compressionType
			if autoDetect {
				currentFileCompressionType = fct.lookupByMimeSubType(filepath.Ext(fileName))
				encoding := ""
				//mimeType := mime.TypeByExtension(filepath.Ext(fileName))
				//fmt.Println(mimeType)
				test := make([]byte, 4)
				if encoding == "" {
					if sfa.sourceStream != nil {
						sfa.sourceStream.Read(test)
					}
					if fileName[len(fileName)-3:] == ".br" {
						encoding = "br"
					} else if len(test) > 0 && bytes.Equal(test[:3], []byte{'O', 'R', 'C'}) {
						encoding = "orc"
					} else if len(test) > 0 && bytes.Equal(test, []byte{'P', 'A', 'R', '1'}) {
						encoding = "parquet"
					}
				}

				if encoding != "" {
					currentFileCompressionType = fct.lookupByMimeSubType(encoding)
				}

				if currentFileCompressionType != nil && !currentFileCompressionType.isSupported {
					return &SnowflakeError{
						Number: ErrCompressionNotSupported,
						Message: fmt.Sprintf("feature is not supported: %v",
							currentFileCompressionType),
					}
				}
			} else {
				currentFileCompressionType = userSpecifiedSourceCompression
			}

			if currentFileCompressionType != nil {
				meta.srcCompressionType = currentFileCompressionType
				if currentFileCompressionType.isSupported {
					meta.dstCompressionType = currentFileCompressionType
					meta.requireCompress = false
					meta.dstFileName = meta.name
				} else {
					return &SnowflakeError{
						Number: ErrCompressionNotSupported,
						Message: fmt.Sprintf("feature is not supported: %v",
							currentFileCompressionType),
					}
				}
			} else {
				meta.requireCompress = sfa.autoCompress
				meta.srcCompressionType = nil
				if sfa.autoCompress {
					dstFileName := meta.name + compressionTypes["GZIP"].fileExtension
					meta.dstFileName = dstFileName
					meta.dstCompressionType = &gzipCompression
				} else {
					meta.dstFileName = meta.name
					meta.dstCompressionType = nil
				}
			}
		}
	}
	return nil
}

func (sfa *snowflakeFileTransferAgent) getLocalFilePathFromCommand(command string) string {
	if len(command) == 0 || !strings.Contains(command, fileProtocol) {
		return ""
	}
	if regexp.MustCompile(putRegexp).Match([]byte(command)) {
		return ""
	}

	filePathBeginIdx := strings.Index(command, fileProtocol)
	isFilePathQuoted := command[filePathBeginIdx-1] == '\''
	filePathBeginIdx += len(fileProtocol)

	filePath := ""
	var filePathEndIdx = 0

	if isFilePathQuoted {
		filePathEndIdx = strings.Index(command[filePathBeginIdx:], "'")
		if filePathEndIdx > filePathBeginIdx {
			filePath = command[filePathBeginIdx:filePathEndIdx]
		}
	} else {
		indexList := make([]int, 0)
		delims := []rune{' ', '\n', ';'}
		for _, delim := range delims {
			index := strings.Index(command[filePathBeginIdx:], string(delim))
			if index != -1 {
				indexList = append(indexList, index)
			}
		}
		filePathEndIdx = -1
		if getMin(indexList) != -1 {
			filePathEndIdx = getMin(indexList)
		}

		if filePathEndIdx > filePathBeginIdx {
			filePath = command[filePathBeginIdx:filePathEndIdx]
		} else {
			filePath = command[filePathBeginIdx:]
		}
	}
	return filePath
}

func (sfa *snowflakeFileTransferAgent) upload(largeFileMetadata []*fileMetadata, smallFileMetadata []*fileMetadata) {
	storageClient := sfa.getStorageClient(sfa.stageLocationType)

	for _, meta := range smallFileMetadata {
		meta.client = storageClient
	}
	for _, meta := range largeFileMetadata {
		meta.client = storageClient
	}

	if len(smallFileMetadata) > 0 {
		sfa.uploadFilesParallel(smallFileMetadata)
	}
	if len(largeFileMetadata) > 0 {
		sfa.uploadFilesSequential(largeFileMetadata)
	}
}

func (sfa *snowflakeFileTransferAgent) download(largeFileMetadata []fileMetadata, smallFileMetadata []fileMetadata) {
	// TODO SNOW-206124
}

func (sfa *snowflakeFileTransferAgent) uploadFilesParallel(fileMetas []*fileMetadata) {
	idx := 0
	fileMetaLen := len(fileMetas)
	for idx < fileMetaLen {
		endOfIdx := intMin(fileMetaLen, idx+int(sfa.parallel))
		targetMeta := fileMetas[idx:endOfIdx]
		for {
			var wg sync.WaitGroup
			results := make([]*fileMetadata, fileMetaLen)
			for i, meta := range targetMeta {
				wg.Add(1)
				go func(k int, m *fileMetadata) {
					defer wg.Done()
					results[k] = sfa.uploadOneFile(m)
				}(i, meta)
			}
			wg.Wait()

			retryMeta := make([]*fileMetadata, 0)
			for _, result := range results {
				if result.resStatus == renewToken || result.resStatus == renewPresignedURL {
					retryMeta = append(retryMeta, result)
				} else {
					sfa.results = append(sfa.results, result)
				}
			}

			if len(retryMeta) == 0 {
				break
			}
			for _, result := range retryMeta {
				if result.resStatus == renewPresignedURL {
					break
				}
			}
			targetMeta = retryMeta
		}
		if endOfIdx == fileMetaLen {
			break
		}
		idx += int(sfa.parallel)
	}
}

func (sfa *snowflakeFileTransferAgent) uploadFilesSequential(fileMetas []*fileMetadata) {
	idx := 0
	fileMetaLen := len(fileMetas)
	for idx < fileMetaLen {
		res := sfa.uploadOneFile(fileMetas[idx])
		sfa.results = append(sfa.results, res)
		idx++
		if injectWaitPut > 0 {
			time.Sleep(injectWaitPut)
		}
	}
}

func (sfa *snowflakeFileTransferAgent) uploadOneFile(meta *fileMetadata) *fileMetadata {
	fmt.Println("upload one file")
	meta.realSrcFileName = meta.srcFileName
	tmpDir := os.TempDir()
	meta.tmpDir = tmpDir
	defer os.RemoveAll(tmpDir) // cleanup

	fileUtil := new(snowflakeFileUtil)
	if meta.requireCompress {
		if meta.srcStream != nil {
			meta.realSrcStream, _ = fileUtil.compressFileWithGzipFromStream(&meta.srcStream)
		} else {
			panic("not implemented") // TODO SNOW-29352
		}
	}

	if meta.srcStream != nil {
		if meta.realSrcStream != nil {
			meta.sha256Digest, meta.uploadSize = fileUtil.getDigestAndSizeForStream(&meta.realSrcStream)
		} else {
			meta.sha256Digest, meta.uploadSize = fileUtil.getDigestAndSizeForStream(&meta.srcStream)
		}

	} else {
		panic("not implemented") // TODO SNOW-29352
	}

	client := sfa.getStorageClient(sfa.stageLocationType)
	client.uploadOneFileWithRetry(meta)
	return meta
}

func (sfa *snowflakeFileTransferAgent) downloadFilesParallel(fileMetas []fileMetadata) {
	// TODO SNOW-206124
}

func (sfa *snowflakeFileTransferAgent) downloadFilesSequential(fileMetas []fileMetadata) {
	// TODO SNOW-206124
}

func (sfa *snowflakeFileTransferAgent) downloadOneFile(meta fileMetadata) fileMetadata {
	// TODO SNOW-206124
	return fileMetadata{}
}

func (sfa *snowflakeFileTransferAgent) getStorageClient(stageLocationType cloudType) storageUtil {
	if stageLocationType == local {
		return &localUtil{}
	} else if stageLocationType == s3Client {
		return nil // TODO SNOW-29352
	} else if stageLocationType == azureClient {
		return nil // TODO SNOW-29352
	} else if stageLocationType == gcsClient {
		return nil // TODO SNOW-29352
	}
	return nil
}

func (sfa *snowflakeFileTransferAgent) result() (*execResponseData, error) {
	data := new(execResponseData)
	rowset := make([]fileTransferResultType, 0)
	if sfa.commandType == uploadCommand {
		if len(sfa.results) > 0 {
			for _, meta := range sfa.results {
				var srcCompressionType, dstCompressionType *compressionType
				if meta.srcCompressionType != nil {
					srcCompressionType = meta.srcCompressionType
				} else {
					srcCompressionType = &compressionType{
						name: "NONE",
					}
				}
				if meta.dstCompressionType != nil {
					dstCompressionType = meta.dstCompressionType
				} else {
					dstCompressionType = &compressionType{
						name: "NONE",
					}
				}
				errorDetails := meta.errorDetails
				srcFileSize := meta.srcFileSize
				dstFileSize := meta.dstFileSize
				if sfa.raisePutGetError && errorDetails != nil {
					return nil, &SnowflakeError{
						Number:  ErrFailedToUploadToStage,
						Message: errorDetails.Error(),
					}
				}
				rowset = append(rowset, fileTransferResultType{
					meta.name,
					meta.srcFileName,
					meta.dstFileName,
					srcFileSize,
					dstFileSize,
					srcCompressionType,
					dstCompressionType,
					meta.resStatus,
					meta.errorDetails,
				})
			}
			sort.Slice(rowset, func(i, j int) bool {
				return rowset[i].srcFileName < rowset[j].srcFileName
			})
			ccrs := make([][]*string, 0, len(rowset))
			for _, rs := range rowset {
				srcFileSize := fmt.Sprintf("%v", rs.srcFileSize)
				dstFileSize := fmt.Sprintf("%v", rs.dstFileSize)
				resStatus := rs.resStatus.String()
				errorStr := ""
				if rs.errorDetails != nil {
					errorStr = rs.errorDetails.Error()
				}
				ccrs = append(ccrs, []*string{
					&rs.srcFileName,
					&rs.dstFileName,
					&srcFileSize,
					&dstFileSize,
					&rs.srcCompressionType.name,
					&rs.dstCompressionType.name,
					&resStatus,
					&errorStr,
				})
			}
			data.RowSet = ccrs
			cc := make([]chunkRowType, len(ccrs))
			populateJSONRowSet(cc, ccrs)
			rt := []execResponseRowType{
				{Name: "source", ByteLength: 10000, Length: 10000, Type: "TEXT", Scale: 0, Nullable: false},
				{Name: "target", ByteLength: 10000, Length: 10000, Type: "TEXT", Scale: 0, Nullable: false},
				{Name: "source_size", ByteLength: 64, Length: 64, Type: "FIXED", Scale: 0, Nullable: false},
				{Name: "target_size", ByteLength: 64, Length: 64, Type: "FIXED", Scale: 0, Nullable: false},
				{Name: "source_compression", ByteLength: 10000, Length: 10000, Type: "TEXT", Scale: 0, Nullable: false},
				{Name: "target_compression", ByteLength: 10000, Length: 10000, Type: "TEXT", Scale: 0, Nullable: false},
				{Name: "status", ByteLength: 10000, Length: 10000, Type: "TEXT", Scale: 0, Nullable: false},
				{Name: "message", ByteLength: 10000, Length: 10000, Type: "TEXT", Scale: 0, Nullable: false},
			}
			data.RowType = rt
			//rows.ChunkDownloader = &snowflakeChunkDownloader{
			//	ctx:                context.Background(),
			//	CurrentChunk:       cc,
			//	Total:              int64(len(cc)),
			//	ChunkMetas:         []execResponseChunk{},
			//	TotalRowIndex:      -1,
			//	Qrmk:               "",
			//	RowSet:             rowSetType{RowType: rt},
			//	FuncDownload:       nil,
			//	FuncDownloadHelper: nil,
			//}
			return data, nil
		}
	}
	return new(execResponseData), nil
}

func isFileTransfer(query string) bool {
	putRe := regexp.MustCompile(putRegexp)
	getRe := regexp.MustCompile(getRegexp)
	return putRe.Match([]byte(query)) || getRe.Match([]byte(query))
}
