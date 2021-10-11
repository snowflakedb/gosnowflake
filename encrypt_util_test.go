// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

const timeFormat = "2006-01-02T15:04:05"

func TestEncryptDecryptFile(t *testing.T) {
	encMat := snowflakeFileEncryption{
		"ztke8tIdVt1zmlQIZm0BMA==",
		"123873c7-3a66-40c4-ab89-e3722fbccce1",
		3112,
	}
	data := "test data"
	inputFile := "test_encrypt_decrypt_file"

	fd, err := os.OpenFile(inputFile, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		t.Error(err)
	}
	defer fd.Close()
	defer os.Remove(inputFile)
	if _, err = fd.Write([]byte(data)); err != nil {
		t.Error(err)
	}

	metadata, encryptedFile, err := encryptFile(&encMat, inputFile, 0, "")
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(encryptedFile)
	decryptedFile, err := decryptFile(metadata, &encMat, encryptedFile, 0, "")
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(decryptedFile)

	fd, _ = os.OpenFile(decryptedFile, os.O_RDONLY, os.ModePerm)
	defer fd.Close()
	content, _ := ioutil.ReadAll(fd)
	if string(content) != data {
		t.Fatalf("data did not match content. expected: %v, got: %v", data, string(content))
	}
}

func TestEncryptDecryptLargeFile(t *testing.T) {
	encMat := snowflakeFileEncryption{
		"ztke8tIdVt1zmlQIZm0BMA==",
		"123873c7-3a66-40c4-ab89-e3722fbccce1",
		3112,
	}
	numberOfFiles := 1
	numberOfLines := 10000
	tmpDir, _ := ioutil.TempDir("", "data")
	tmpDir = generateKLinesOfNFiles(numberOfLines, numberOfFiles, false, tmpDir)
	defer os.RemoveAll(tmpDir)
	files, err := filepath.Glob(filepath.Join(tmpDir, "file*"))
	if err != nil {
		t.Error(err)
	}
	inputFile := files[0]

	metadata, encryptedFile, err := encryptFile(&encMat, inputFile, 0, tmpDir)
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(encryptedFile)
	decryptedFile, err := decryptFile(metadata, &encMat, encryptedFile, 0, tmpDir)
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(decryptedFile)

	cnt := 0
	fd, _ := os.OpenFile(decryptedFile, os.O_RDONLY, os.ModePerm)
	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		cnt++
	}
	if err = scanner.Err(); err != nil {
		t.Error(err)
	}
	if cnt != numberOfLines {
		t.Fatalf("incorrect number of lines. expected: %v, got: %v", numberOfLines, cnt)
	}
}

func generateKLinesOfNFiles(k int, n int, compress bool, tmpDir string) string {
	if tmpDir == "" {
		tmpDir, _ = ioutil.TempDir(tmpDir, "data")
	}
	for i := 0; i < n; i++ {
		fname := path.Join(tmpDir, "file"+strconv.FormatInt(int64(i), 10))
		f, _ := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		for j := 0; j < k; j++ {
			num := rand.Float64() * 10000
			min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
			max := time.Date(2070, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
			delta := max - min
			sec := rand.Int63n(delta) + min
			tm := time.Unix(sec, 0)
			dt := tm.Format("2021-03-01")
			sec = rand.Int63n(delta) + min
			ts := time.Unix(sec, 0).Format(timeFormat)
			sec = rand.Int63n(delta) + min
			tsltz := time.Unix(sec, 0).Format(timeFormat)
			sec = rand.Int63n(delta) + min
			tsntz := time.Unix(sec, 0).Format(timeFormat)
			sec = rand.Int63n(delta) + min
			tstz := time.Unix(sec, 0).Format(timeFormat)
			pct := rand.Float64() * 1000
			ratio := fmt.Sprintf("%.2f", rand.Float64()*1000)
			rec := fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v\n", num, dt, ts, tsltz, tsntz, tstz, pct, ratio)
			f.Write([]byte(rec))
		}
		f.Close()
		if compress {
			if !isWindows {
				gzipCmd := exec.Command("gzip", filepath.Join(tmpDir, "file"+strconv.FormatInt(int64(i), 10)))
				gzipOut, _ := gzipCmd.StdoutPipe()
				gzipErr, _ := gzipCmd.StderrPipe()
				gzipCmd.Start()
				ioutil.ReadAll(gzipOut)
				ioutil.ReadAll(gzipErr)
				gzipCmd.Wait()
			} else {
				fOut, _ := os.OpenFile(fname+".gz", os.O_CREATE|os.O_WRONLY, os.ModePerm)
				w := gzip.NewWriter(fOut)
				fIn, _ := os.OpenFile(fname, os.O_RDONLY, os.ModePerm)
				if _, err := io.Copy(w, fIn); err != nil {
					return ""
				}
				w.Close()
			}
		}
	}
	return tmpDir
}
