// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

//lint:file-ignore U1000 TODO SNOW-29352

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"io/ioutil"
	"net/url"
	usr "os/user"
	"path/filepath"
	"strings"
)

type snowflakeFileUtil struct {
}

func (util *snowflakeFileUtil) compressFileWithGzipFromStream(srcStream **bytes.Buffer) (*bytes.Buffer, int) {
	r := getReaderFromBuffer(srcStream)
	buf, _ := ioutil.ReadAll(r)
	var c bytes.Buffer
	w := gzip.NewWriter(&c)
	w.Write(buf) // write buf to gzip writer
	w.Close()
	return &c, c.Len()
}

func (util *snowflakeFileUtil) getDigestAndSize(src **bytes.Buffer) (string, int64) {
	chunkSize := 16 * 4 * 1024
	m := sha256.New()
	r := getReaderFromBuffer(src)
	for {
		chunk := make([]byte, chunkSize)
		n, err := r.Read(chunk)
		if n == 0 || err != nil {
			break
		}
		m.Write(chunk[:n])
	}
	return base64.StdEncoding.EncodeToString(m.Sum(nil)), int64((*src).Len())
}

func (util *snowflakeFileUtil) getDigestAndSizeForStream(stream **bytes.Buffer) (string, int64) {
	return util.getDigestAndSize(stream)
}

func (util *snowflakeFileUtil) getDigestAndSizeForFile(fileName string) (string, int64) {
	src, err := ioutil.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	buf := bytes.NewBuffer(src)
	return util.getDigestAndSize(&buf)
}

// file metadata for PUT/GET
type fileMetadata struct {
	name              string
	srcFileName       string
	srcFileSize       int
	stageLocationType cloudType
	resStatus         resultStatus

	stageInfo                       *execResponseStageInfo
	srcCompressionType              *compressionType
	dstCompressionType              *compressionType
	requireCompress                 bool
	dstFileName                     string
	parallel                        int64
	sha256Digest                    string
	uploadSize                      int64
	encryptionMaterial              *snowflakeFileEncryption
	dstFileSize                     int64
	overwrite                       bool
	sfa                             *snowflakeFileTransferAgent
	client                          cloudClient // GetObjectOutput (S3), ContainerURL (Azure), string (GCS)
	realSrcFileName                 string
	tmpDir                          string
	presignedURL                    *url.URL
	errorDetails                    error
	lastError                       error
	noSleepingTime                  bool
	lastMaxConcurrency              int
	localLocation                   string
	localDigest                     string
	showProgressBar                 bool
	gcsFileHeaderDigest             string
	gcsFileHeaderContentLength      int
	gcsFileHeaderEncryptionMeta *encryptMetadata

	/* streaming PUT */
	srcStream     *bytes.Buffer
	realSrcStream *bytes.Buffer

	/* PUT */
	putCallback             *snowflakeProgressPercentage
	putAzureCallback        *snowflakeProgressPercentage
	putCallbackOutputStream *io.Writer

	/* GET */
	getCallback             *snowflakeProgressPercentage
	getAzureCallback        *snowflakeProgressPercentage
	getCallbackOutputStream *io.Writer
}

type fileTransferResultType struct {
	name               string
	srcFileName        string
	dstFileName        string
	srcFileSize        int
	dstFileSize        int64
	srcCompressionType *compressionType
	dstCompressionType *compressionType
	resStatus          resultStatus
	errorDetails       error
}

type fileHeader struct {
	digest             string
	contentLength      int64
	encryptionMetadata *encryptMetadata
}

func getReaderFromBuffer(src **bytes.Buffer) io.Reader {
	var b bytes.Buffer
	tee := io.TeeReader(*src, &b) // read src to buf
	*src = &b                     // revert pointer back
	return tee
}

func baseName(path string) string {
	base := filepath.Base(path)
	if base == "." || base == "/" {
		return ""
	}
	if len(base) > 1 && (path[len(path)-1:] == "." || path[len(path)-1:] == "/") {
		return ""
	}
	return base
}

func expandUser(path string) string {
	usr, _ := usr.Current()
	dir := usr.HomeDir
	if path == "~" {
		path = dir
	} else if strings.HasPrefix(path, "~/") {
		path = filepath.Join(dir, path[2:])
	}
	return path
}
