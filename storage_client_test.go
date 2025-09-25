package gosnowflake

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestProcessEncryptedFileToDestination_DecryptionFailure tests that temporary files
// are cleaned up when decryption fails due to invalid encryption data
func TestProcessEncryptedFileToDestination_DecryptionFailure(t *testing.T) {
	tmpDir := t.TempDir()
	fullDstFileName := filepath.Join(tmpDir, "final_destination.txt")
	tempDownloadFile := fullDstFileName + ".tmp"
	assertNilF(t, os.WriteFile(tempDownloadFile, []byte("invalid encrypted content"), 0644), "Failed to create temp download file")

	// Create metadata with invalid encryption material to trigger decryption failure
	meta := &fileMetadata{
		encryptionMaterial: &snowflakeFileEncryption{
			QueryStageMasterKey: "invalid_key", // Invalid key to cause decryption failure
			QueryID:             "test-query-id",
			SMKID:               12345,
		},
		tmpDir:      tmpDir,
		srcFileName: "test_file.txt",
		options: &SnowflakeFileTransferOptions{
			GetFileToStream: false,
		},
	}

	// Create header with invalid encryption metadata
	header := &fileHeader{
		encryptionMetadata: &encryptMetadata{
			key:     "invalid_key_data", // Invalid encryption data
			iv:      "invalid_iv_data",
			matdesc: `{"smkId":"12345","queryId":"test-query-id","keySize":"256"}`,
		},
	}

	// Test: decryption should fail due to invalid encryption data
	rsu := &remoteStorageUtil{}
	err := rsu.processEncryptedFileToDestination(meta, header, tempDownloadFile, fullDstFileName)
	assertNotNilF(t, err, "Expected decryption to fail with invalid encryption data")

	// Verify that the final destination file was not created
	_, err = os.Stat(fullDstFileName)
	assertTrueF(t, os.IsNotExist(err), "Final destination file should not exist after decryption failure")

	// Verify the temp download file was cleaned up even though decryption failed
	_, err = os.Stat(tempDownloadFile)
	assertTrueF(t, os.IsNotExist(err), "Temp download file should be cleaned up even after decryption failure")

	verifyNoTmpFilesLeftBehind(t, fullDstFileName)
}

// TestProcessEncryptedFileToDestination_Success tests successful decryption and file handling
func TestProcessEncryptedFileToDestination_Success(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test data and encrypt it properly
	inputData := "test data for successful encryption/decryption"
	inputFile := filepath.Join(tmpDir, "input.txt")
	assertNilF(t, os.WriteFile(inputFile, []byte(inputData), 0644), "Failed to create input file")

	// Create valid encryption material
	encMat := &snowflakeFileEncryption{
		QueryStageMasterKey: "ztke8tIdVt1zmlQIZm0BMA==",
		QueryID:             "test-query-id",
		SMKID:               12345,
	}

	// Encrypt the file to create valid encrypted content
	metadata, encryptedFile, err := encryptFileCBC(encMat, inputFile, 0, tmpDir)
	assertNilF(t, err, "Failed to encrypt test file")
	defer os.Remove(encryptedFile)

	// Create final destination path
	fullDstFileName := filepath.Join(tmpDir, "final_destination.txt")

	// Create metadata for decryption
	meta := &fileMetadata{
		encryptionMaterial: encMat,
		tmpDir:             tmpDir,
		srcFileName:        "test_file.txt",
		options: &SnowflakeFileTransferOptions{
			GetFileToStream: false,
		},
	}

	header := &fileHeader{
		encryptionMetadata: metadata,
	}

	// Test: successful decryption and file move
	rsu := &remoteStorageUtil{}
	err = rsu.processEncryptedFileToDestination(meta, header, encryptedFile, fullDstFileName)
	assertNilF(t, err, "Expected successful decryption and file move")

	// Verify that the final destination file was created with correct content
	finalContent, err := os.ReadFile(fullDstFileName)
	assertNilF(t, err, "Failed to read final destination file")
	assertEqualF(t, string(finalContent), inputData, "Final file content should match original input")

	// Verify the final destination file exists and has correct content
	_, err = os.Stat(fullDstFileName)
	assertNilF(t, err, "Final destination file should exist")

	verifyNoTmpFilesLeftBehind(t, fullDstFileName)
}

func verifyNoTmpFilesLeftBehind(t *testing.T, fullDstFileName string) {
	destDir := filepath.Dir(fullDstFileName)
	files, err := os.ReadDir(destDir)
	assertNilF(t, err, "Failed to read destination directory")

	tmpFileCount := 0
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".tmp") {
			tmpFileCount++
		}
	}
	assertEqualF(t, tmpFileCount, 0, "No .tmp files should remain in destination directory after successful operation")
}
