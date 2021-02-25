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

func encryptStream(sfe *snowflakeFileEncryption, src *bytes.Buffer, out *bytes.Buffer, chunkSize int) *encryptMetadata {
	if chunkSize == 0 {
		chunkSize = aes.BlockSize * 4 * 1024
	}
	decodedKey, _ := base64.StdEncoding.DecodeString(sfe.QueryStageMasterKey)
	keySize := len(decodedKey)

	ivData := getSecureRandom(aes.BlockSize)
	fileKey := getSecureRandom(keySize)

	block, err := aes.NewCipher(fileKey)
	if err != nil {
		panic(err)
	}
	mode := cipher.NewCBCEncrypter(block, ivData)
	cipherText := make([]byte, aes.BlockSize+src.Len()) // sizeOf(initializationVector) + sizeOf(src)

	padded := false
	for {
		buf := getByteBufferContent(&src)
		if len(*buf) == 0 {
			break
		} else if len(*buf) % aes.BlockSize != 0 {
			*buf = padBytesLength(*buf, aes.BlockSize)
			padded = true
		}
		mode.CryptBlocks(cipherText[aes.BlockSize:], *buf)
		out.Write(cipherText)
	}
	if !padded {
		blockSizeCipher := make([]byte, aes.BlockSize)
		for i := range blockSizeCipher {
			blockSizeCipher[i] = []byte(string(rune(aes.BlockSize)))[0]
		}
		chunk := make([]byte, aes.BlockSize)
		mode.CryptBlocks(chunk, blockSizeCipher)
		out.Write(chunk)
	}

	// encrypt key with QRMK
	var encKek []byte
	block, _ = aes.NewCipher(decodedKey)
	block.Encrypt(encKek, padBytesLength(fileKey, aes.BlockSize))

	matDesc := materialDescriptor{
		sfe.SMKID,
		sfe.QueryID,
		keySize * 8,
	}

	key, _ := base64.StdEncoding.DecodeString(string(encKek))
	iv, _ := base64.StdEncoding.DecodeString(string(ivData))
	return &encryptMetadata{
		string(key),
		string(iv),
		matdescToUnicode(matDesc),
	}
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

func unpadBytesLength(src []byte) []byte {
	length := len(src)
	unpadding := int(src[length-1])
	return src[:(length - unpadding)]
}
