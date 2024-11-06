// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bufio"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"io"
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

type encryptDecryptTestFile struct {
	numberOfBytesInEachRow int
	numberOfLines          int
}

func TestEncryptDecryptFileCBC(t *testing.T) {
	encMat := snowflakeFileEncryption{
		"ztke8tIdVt1zmlQIZm0BMA==",
		"123873c7-3a66-40c4-ab89-e3722fbccce1",
		3112,
	}
	data := "test data"
	inputFile := "test_encrypt_decrypt_file"

	fd, err := os.Create(inputFile)
	if err != nil {
		t.Error(err)
	}
	defer fd.Close()
	defer os.Remove(inputFile)
	if _, err = fd.Write([]byte(data)); err != nil {
		t.Error(err)
	}

	metadata, encryptedFile, err := encryptFileCBC(&encMat, inputFile, 0, "")
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(encryptedFile)
	decryptedFile, err := decryptFileCBC(metadata, &encMat, encryptedFile, 0, "")
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(decryptedFile)

	fd, err = os.Open(decryptedFile)
	if err != nil {
		t.Error(err)
	}
	defer fd.Close()
	content, err := io.ReadAll(fd)
	if err != nil {
		t.Error(err)
	}
	if string(content) != data {
		t.Fatalf("data did not match content. expected: %v, got: %v", data, string(content))
	}
}

func TestEncryptDecryptFilePadding(t *testing.T) {
	encMat := snowflakeFileEncryption{
		"ztke8tIdVt1zmlQIZm0BMA==",
		"123873c7-3a66-40c4-ab89-e3722fbccce1",
		3112,
	}

	testcases := []encryptDecryptTestFile{
		// File size is a multiple of 65536 bytes (chunkSize)
		{numberOfBytesInEachRow: 8, numberOfLines: 16384},
		{numberOfBytesInEachRow: 16, numberOfLines: 4096},
		// File size is not a multiple of 65536 bytes (chunkSize)
		{numberOfBytesInEachRow: 8, numberOfLines: 10240},
		{numberOfBytesInEachRow: 16, numberOfLines: 6144},
		// The second chunk's size is a multiple of 16 bytes (aes.BlockSize)
		{numberOfBytesInEachRow: 16, numberOfLines: 4097},
		// The second chunk's size is not a multiple of 16 bytes (aes.BlockSize)
		{numberOfBytesInEachRow: 12, numberOfLines: 5462},
		{numberOfBytesInEachRow: 10, numberOfLines: 6556},
	}

	for _, test := range testcases {
		t.Run(fmt.Sprintf("%v_%v", test.numberOfBytesInEachRow, test.numberOfLines), func(t *testing.T) {
			tmpDir, err := generateKLinesOfNByteRows(test.numberOfLines, test.numberOfBytesInEachRow, t.TempDir())
			if err != nil {
				t.Error(err)
			}

			encryptDecryptFile(t, encMat, test.numberOfLines, tmpDir)
		})
	}
}

func TestEncryptDecryptLargeFileCBC(t *testing.T) {
	encMat := snowflakeFileEncryption{
		"ztke8tIdVt1zmlQIZm0BMA==",
		"123873c7-3a66-40c4-ab89-e3722fbccce1",
		3112,
	}

	numberOfFiles := 1
	numberOfLines := 10000
	tmpDir, err := generateKLinesOfNFiles(numberOfLines, numberOfFiles, false, t.TempDir())
	if err != nil {
		t.Error(err)
	}

	encryptDecryptFile(t, encMat, numberOfLines, tmpDir)
}

func encryptDecryptFile(t *testing.T, encMat snowflakeFileEncryption, expected int, tmpDir string) {
	files, err := filepath.Glob(filepath.Join(tmpDir, "file*"))
	if err != nil {
		t.Error(err)
	}
	inputFile := files[0]

	metadata, encryptedFile, err := encryptFileCBC(&encMat, inputFile, 0, tmpDir)
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(encryptedFile)
	decryptedFile, err := decryptFileCBC(metadata, &encMat, encryptedFile, 0, tmpDir)
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(decryptedFile)

	cnt := 0
	fd, err := os.Open(decryptedFile)
	if err != nil {
		t.Error(err)
	}
	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		cnt++
	}
	if err = scanner.Err(); err != nil {
		t.Error(err)
	}
	if cnt != expected {
		t.Fatalf("incorrect number of lines. expected: %v, got: %v", expected, cnt)
	}
}

func generateKLinesOfNByteRows(numLines int, numBytes int, tmpDir string) (string, error) {
	fname := path.Join(tmpDir, "file"+strconv.FormatInt(int64(numLines*numBytes), 10))
	f, err := os.Create(fname)
	if err != nil {
		return "", err
	}

	for j := 0; j < numLines; j++ {
		str := randomString(numBytes - 1) // \n is the last character
		rec := fmt.Sprintf("%v\n", str)
		if _, err = f.Write([]byte(rec)); err != nil {
			return "", err
		}
	}
	err = f.Close()
	return tmpDir, err
}

func generateKLinesOfNFiles(k int, n int, compress bool, tmpDir string) (string, error) {
	for i := 0; i < n; i++ {
		fname := path.Join(tmpDir, "file"+strconv.FormatInt(int64(i), 10))
		f, err := os.Create(fname)
		if err != nil {
			return "", err
		}
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
			if _, err = f.Write([]byte(rec)); err != nil {
				return "", err
			}
		}
		if err = f.Close(); err != nil {
			return "", err
		}
		if compress {
			if !isWindows {
				gzipCmd := exec.Command("gzip", filepath.Join(tmpDir, "file"+strconv.FormatInt(int64(i), 10)))
				gzipOut, err := gzipCmd.StdoutPipe()
				if err != nil {
					return "", err
				}
				gzipErr, err := gzipCmd.StderrPipe()
				if err != nil {
					return "", err
				}
				if err = gzipCmd.Start(); err != nil {
					return "", err
				}
				if _, err = io.ReadAll(gzipOut); err != nil {
					return "", err
				}
				if _, err = io.ReadAll(gzipErr); err != nil {
					return "", err
				}
				if err = gzipCmd.Wait(); err != nil {
					return "", err
				}
			} else {
				fOut, err := os.Create(fname + ".gz")
				if err != nil {
					return "", err
				}
				w := gzip.NewWriter(fOut)
				fIn, err := os.Open(fname)
				if err != nil {
					return "", err
				}
				if _, err = io.Copy(w, fIn); err != nil {
					return "", err
				}
				w.Close()
				fOut.Close()
				fIn.Close()
			}
		}
	}
	return tmpDir, nil
}

func TestEncryptDecryptGCM(t *testing.T) {
	input := []byte("abc")
	iv := []byte("ab1234567890")      // pragma: allowlist secret
	key := []byte("1234567890abcdef") // pragma: allowlist secret
	encrypted, err := encryptGCM(iv, input, key, nil)
	assertNilF(t, err)
	assertEqualE(t, base64.StdEncoding.EncodeToString(encrypted), "iG+lT4o27hkzj3kblYRzQikLVQ==")

	decrypted, err := decryptGCM(iv, encrypted, key, nil)
	assertNilF(t, err)
	assertDeepEqualE(t, decrypted, input)
}

func TestEncryptDecryptFileGCM(t *testing.T) {
	tmpDir := os.TempDir()
	tempFile, err := os.CreateTemp(tmpDir, "gcm")
	assertNilF(t, err)
	_, err = tempFile.Write([]byte("abc"))
	assertNilF(t, err)

	sfe := &snowflakeFileEncryption{
		QueryStageMasterKey: "YWJjZGVmMTIzNDU2Nzg5MA==",
		QueryID:             "unused",
		SMKID:               123,
	}
	meta, encryptedFileName, err := encryptFileGCM(sfe, tempFile.Name(), tmpDir)
	assertNilF(t, err)

	decryptedFileName, err := decryptFileGCM(meta, sfe, encryptedFileName, tmpDir)
	assertNilF(t, err)

	fileContent, err := os.ReadFile(decryptedFileName)
	assertNilF(t, err)

	assertEqualE(t, string(fileContent), "abc")
}
