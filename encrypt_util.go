// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

//lint:file-ignore U1000 TODO SNOW-29352

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
)

type snowflakeFileEncryption struct {
	QueryStageMasterKey string `json:"queryStageMasterKey,omitempty"`
	QueryID             string `json:"queryId,omitempty"`
	SMKID               int64  `json:"smkId,omitempty"`
}

// PUT requests return a single encryptionMaterial object whereas GET requests
// return a slice (array) of encryptionMaterial objects, both under the field
// 'encryptionMaterial'
type encryptionWrapper struct {
	snowflakeFileEncryption
	EncryptionMaterials []snowflakeFileEncryption
}

// override default behavior for wrapper
func (ew *encryptionWrapper) UnmarshalJSON(data []byte) error {
	// if GET, unmarshal slice of encryptionMaterial
	if err := json.Unmarshal(data, &ew.EncryptionMaterials); err == nil {
		return err
	}
	// else (if PUT), unmarshal the encryptionMaterial itself
	return json.Unmarshal(data, &ew.snowflakeFileEncryption)
}

type encryptMetadata struct {
	key     string
	iv      string
	matdesc string
}

// encryptStream encrypts a stream buffer using AES128 block cipher in CBC mode
// with PKCS5 padding
func encryptStream(
	sfe *snowflakeFileEncryption,
	src io.Reader,
	out io.Writer,
	chunkSize int) (*encryptMetadata, error) {
	if chunkSize == 0 {
		chunkSize = aes.BlockSize * 4 * 1024
	}
	decodedKey, _ := base64.StdEncoding.DecodeString(sfe.QueryStageMasterKey)
	keySize := len(decodedKey)

	ivData := getSecureRandom(aes.BlockSize)
	fileKey := getSecureRandom(keySize)

	block, _ := aes.NewCipher(fileKey)
	mode := cipher.NewCBCEncrypter(block, ivData)
	cipherText := make([]byte, chunkSize)

	// encrypt file with CBC
	var err error
	padded := false
	for {
		chunk := make([]byte, chunkSize)
		n, err := src.Read(chunk)
		if n == 0 || err != nil {
			break
		} else if n%aes.BlockSize != 0 {
			chunk = padBytesLength(chunk[:n], aes.BlockSize)
			padded = true
		}
		mode.CryptBlocks(cipherText, chunk)
		out.Write(cipherText[:len(chunk)])
	}
	if err != nil {
		return nil, err
	}
	if !padded {
		blockSizeCipher := bytes.Repeat([]byte{byte(aes.BlockSize)}, aes.BlockSize)
		chunk := make([]byte, aes.BlockSize)
		mode.CryptBlocks(chunk, blockSizeCipher)
		out.Write(chunk)
	}

	// encrypt key with CFB
	block, _ = aes.NewCipher(decodedKey)
	stream := cipher.NewCFBEncrypter(block, ivData)
	encryptedFileKey := make([]byte, len(fileKey))
	stream.XORKeyStream(encryptedFileKey, fileKey)

	matDesc := materialDescriptor{
		sfe.SMKID,
		sfe.QueryID,
		keySize * 8,
	}

	return &encryptMetadata{
		base64.StdEncoding.EncodeToString(encryptedFileKey),
		base64.StdEncoding.EncodeToString(ivData),
		matdescToUnicode(matDesc),
	}, nil
}

func encryptFile(sfe *snowflakeFileEncryption, filename string, chunkSize int, tmpDir string) (*encryptMetadata, string, error) {
	if chunkSize == 0 {
		chunkSize = aes.BlockSize * 4 * 1024
	}
	tmpOutputFile, _ := ioutil.TempFile(tmpDir, baseName(filename)+"#")
	infile, err := os.OpenFile(filename, os.O_CREATE|os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, "", err
	}
	meta, err := encryptStream(sfe, infile, tmpOutputFile, chunkSize)
	if err != nil {
		return nil, "", err
	}
	return meta, tmpOutputFile.Name(), nil
}

func decryptFile(metadata *encryptMetadata, sfe *snowflakeFileEncryption, filename string, chunkSize int, tmpDir string) (string, error) {
	if chunkSize == 0 {
		chunkSize = aes.BlockSize * 4 * 1024
	}
	decodedKey, _ := base64.StdEncoding.DecodeString(sfe.QueryStageMasterKey)
	keyBytes, _ := base64.StdEncoding.DecodeString(metadata.key) // encrypted file key
	ivBytes, _ := base64.StdEncoding.DecodeString(metadata.iv)

	// decrypt file key
	var err error
	block, _ := aes.NewCipher(decodedKey)
	stream := cipher.NewCFBDecrypter(block, ivBytes)
	fileKey := keyBytes
	stream.XORKeyStream(fileKey, fileKey)

	// decrypt file
	block, _ = aes.NewCipher(fileKey)
	mode := cipher.NewCBCDecrypter(block, ivBytes)

	tmpOutputFile, err := ioutil.TempFile(tmpDir, baseName(filename)+"#")
	if err != nil {
		return "", err
	}
	infile, err := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return "", err
	}
	var totalFileSize int
	var prevChunk []byte
	for {
		chunk := make([]byte, chunkSize)
		n, err := infile.Read(chunk)
		if n == 0 || err != nil {
			break
		}
		totalFileSize += n
		chunk = chunk[:n]
		mode.CryptBlocks(chunk, chunk)
		tmpOutputFile.Write(chunk)
		prevChunk = chunk
	}
	if err != nil {
		return "", err
	}
	if prevChunk != nil {
		totalFileSize -= paddingOffset(prevChunk)
	}
	tmpOutputFile.Truncate(int64(totalFileSize))
	return tmpOutputFile.Name(), nil
}

type materialDescriptor struct {
	SmkID   int64  `json:"smkId"`
	QueryID string `json:"queryId"`
	KeySize int    `json:"keySize"`
}

func matdescToUnicode(matdesc materialDescriptor) string {
	s, _ := json.Marshal(&matdesc)
	return string(s)
}

func getSecureRandom(byteLength int) []byte {
	token := make([]byte, byteLength)
	rand.Read(token)
	return token
}

func padBytesLength(src []byte, blockSize int) []byte {
	padLength := blockSize - len(src)%blockSize
	padText := bytes.Repeat([]byte{byte(padLength)}, padLength)
	return append(src, padText...)
}

func paddingOffset(src []byte) int {
	length := len(src)
	return int(src[length-1])
}
