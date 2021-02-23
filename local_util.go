package gosnowflake

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
)

type localUtil struct {
}

func (util *localUtil) createClient(info execResponseStageInfo, useAccelerateEndpoint bool) cloudClient {
	return nil
}

func (util *localUtil) uploadOneFileWithRetry(meta *fileMetadata) {
	var frd *bufio.Reader
	if meta.srcStream != nil {
		b := meta.srcStream
		if meta.realSrcStream != nil {
			b = meta.realSrcStream
		}
		frd = bufio.NewReader(b)
	} else {
		f, _ := os.Open(meta.realSrcFileName)
		defer f.Close()
		frd = bufio.NewReader(f)
	}

	if meta.encryptionMaterial != nil {
		panic("not implemented")
	}

	output, err := os.OpenFile(filepath.Join(expandUser(meta.stageInfo.Location), meta.dstFileName), os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer output.Close()
	data := make([]byte, meta.uploadSize)
	for {
		n, err := frd.Read(data)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}

		if _, err = output.Write(data); err != nil {
			panic(err)
		}
	}
	meta.dstFileSize = meta.uploadSize
	meta.resStatus = UPLOADED
}

func (util *localUtil) downloadOneFile() {
	// TODO
}
