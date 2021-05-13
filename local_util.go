// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
)

type localUtil struct {
}

func (util *localUtil) createClient(info *execResponseStageInfo, useAccelerateEndpoint bool) cloudClient {
	return nil
}

func (util *localUtil) uploadOneFileWithRetry(meta *fileMetadata) error {
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

	if !meta.overwrite {
		if _, err := os.Stat(filepath.Join(expandUser(meta.stageInfo.Location), meta.dstFileName)); err == nil {
			meta.dstFileSize = 0
			meta.resStatus = skipped
			return nil
		}
	}
	output, err := os.OpenFile(filepath.Join(expandUser(meta.stageInfo.Location), meta.dstFileName), os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer output.Close()
	data := make([]byte, meta.uploadSize)
	for {
		n, err := frd.Read(data)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		if _, err = output.Write(data); err != nil {
			return err
		}
	}
	meta.dstFileSize = meta.uploadSize
	meta.resStatus = uploaded
	return nil
}

func (util *localUtil) downloadOneFile() {
	// TODO SNOW-206124
}
