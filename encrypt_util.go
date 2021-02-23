// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

//lint:file-ignore U1000 Ignore all unused code

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strconv"
)

const (
	blockSize = int(aes.BlockSize / 8)
)

type snowflakeFileEncryption struct {
	QueryStageMasterKey string `json:"queryStageMasterKey,omitempty"`
	QueryID             string `json:"queryId,omitempty"`
	SMKID               int64  `json:"smkId,omitempty"`
}

func (sfe *snowflakeFileEncryption) encryptStream(src *bytes.Buffer, out *bytes.Buffer, chunkSize int) encryptMetadata {
	if chunkSize == 0 {
		chunkSize = blockSize * 4 * 1024
	}
	decodedKey, _ := base64.StdEncoding.DecodeString(sfe.QueryStageMasterKey)
	keySize := len(decodedKey)

	em := new(encryptMetadata)
	ivData := em.getSecureRandom(blockSize)
	fileKey := em.getSecureRandom(keySize)

	block, err := aes.NewCipher(fileKey)
	if err != nil {
		panic("asdf")
	}
	mode := cipher.NewCBCEncrypter(block, ivData)
	cipherText := make([]byte, aes.BlockSize+src.Len())

	padded := false
	for {
		buf := getByteBufferContent(&src)
		if len(*buf) == 0 {
			break
		} else if len(*buf)&blockSize != 0 {
			*buf = PKCS5Padding(*buf, blockSize)
			padded = true
		}
		mode.CryptBlocks(cipherText[aes.BlockSize:], *buf)
		out.Write(cipherText)
	}
	if !padded {
		blockSizeHex := []byte(strconv.FormatInt(int64(blockSize), 16))
		if len(blockSizeHex) != 1 {
			panic("yikes")
		}
		blockSizeCipher := make([]byte, blockSize)
		for i := range blockSizeCipher {
			blockSizeCipher[i] = blockSizeHex[0]
		}
		chunk := make([]byte, blockSize)
		mode.CryptBlocks(chunk, blockSizeCipher)
		out.Write(chunk)
	}

	// encrypt key with QRMK
	var encKek []byte
	block, _ = aes.NewCipher(decodedKey)
	stream := cipher.NewCFBEncrypter(block, ivData) // TODO look into different encryptor
	fileKey = PKCS5Padding(fileKey, blockSize)
	stream.XORKeyStream(encKek, fileKey)

	matDesc := materialDescriptor{
		sfe.SMKID,
		sfe.QueryID,
		keySize * 8,
	}

	key, _ := base64.StdEncoding.DecodeString(string(encKek))
	iv, _ := base64.StdEncoding.DecodeString(string(ivData))
	return encryptMetadata{
		string(key),
		string(iv),
		matdescToUnicode(matDesc),
	}
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

type materialDescriptor struct {
	SmkID   int64  `json:"smkId"`
	QueryID string `json:"queryId"`
	KeySize int    `json:"keySize"`
}

func matdescToUnicode(matdesc materialDescriptor) string {
	s, _ := json.Marshal(&matdesc)
	return string(s)
}

type encryptMetadata struct {
	key     string
	iv      string
	matdesc string
}

func (em *encryptMetadata) getSecureRandom(byteLength int) []byte {
	token := make([]byte, byteLength)
	rand.Read(token)
	return token
}

func PKCS5Padding(src []byte, blockSize int) []byte {
	padding := blockSize - len(src)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(src, padtext...)
}

func PKCS5UnPadding(src []byte) []byte {
	length := len(src)
	unpadding := int(src[length-1])
	return src[:(length - unpadding)]
}
