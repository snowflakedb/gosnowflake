package gosnowflake

import "testing"

// TestEnsureSingleGetStreamFile checks that a get-stream returns its sole matched file unchanged
// and errors deterministically when the GET matched more than one. No live connection needed: it
// only reads srcFiles/data. Physical paths follow versions/<entity>/<versionId>/<logical>, with a
// per-file <versionId>.
func TestEnsureSingleGetStreamFile(t *testing.T) {
	ws := "versions/29_559629944250378"
	v1 := ws + "/1782146627271"
	v2 := ws + "/1782164111378"
	v3 := ws + "/1782146628397"

	tests := []struct {
		name       string
		srcFiles   []string
		wantFiles  []string // expected srcFiles on success (nil when an error is expected)
		wantErrNum int      // 0 when no error expected
	}{
		{
			name:      "single result is streamed",
			srcFiles:  []string{v1 + "/report.csv"},
			wantFiles: []string{v1 + "/report.csv"},
		},
		{
			name:      "single prefix-sibling is streamed",
			srcFiles:  []string{v1 + "/foobar"},
			wantFiles: []string{v1 + "/foobar"},
		},
		{
			name:       "prefix-sibling match is ambiguous (foo alongside foobar)",
			srcFiles:   []string{v1 + "/foo", v2 + "/foobar"},
			wantErrNum: ErrGetStreamMultipleFiles,
		},
		{
			name:       "file and same-named subdir are ambiguous",
			srcFiles:   []string{v1 + "/foo", v2 + "/foo/foo"},
			wantErrNum: ErrGetStreamMultipleFiles,
		},
		{
			name:       "same leaf in different directories is ambiguous",
			srcFiles:   []string{v1 + "/a/foo", v2 + "/b/foo"},
			wantErrNum: ErrGetStreamMultipleFiles,
		},
		{
			name:       "whole-stage GET with multiple files is ambiguous",
			srcFiles:   []string{v1 + "/a.csv.gz", v2 + "/b.csv.gz"},
			wantErrNum: ErrGetStreamMultipleFiles,
		},
		{
			name:       "three prefix-matched files are ambiguous",
			srcFiles:   []string{v1 + "/foo", v2 + "/foobar", v3 + "/foobaz"},
			wantErrNum: ErrGetStreamMultipleFiles,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sfa := &snowflakeFileTransferAgent{
				commandType: downloadCommand,
				srcFiles:    tt.srcFiles,
				data:        &execResponseData{},
			}

			err := sfa.ensureSingleGetStreamFile()

			if tt.wantErrNum != 0 {
				var sfErr *SnowflakeError
				assertErrorsAsF(t, err, &sfErr)
				assertEqualE(t, sfErr.Number, tt.wantErrNum)
				return
			}
			assertNilF(t, err)
			assertDeepEqualE(t, sfa.srcFiles, tt.wantFiles)
		})
	}
}

// TestEnsureSingleGetStreamFileKeepsEncryptionMaterial guards that the single retained file keeps
// its own encryption material: the check runs after the source-file -> encryption-material map is
// built in parseCommand and, on the single-file path, does not disturb srcFiles ordering.
func TestEnsureSingleGetStreamFileKeepsEncryptionMaterial(t *testing.T) {
	m0 := &snowflakeFileEncryption{SMKID: 100, QueryID: "q0"}
	ws := "versions/29_559629944250378"
	srcFiles := []string{ws + "/1782146627271/alpha"}

	sfa := &snowflakeFileTransferAgent{
		commandType:        downloadCommand,
		srcFiles:           srcFiles,
		encryptionMaterial: []*snowflakeFileEncryption{m0},
		data:               &execResponseData{SrcLocations: srcFiles},
	}

	// Mirror parseCommand: build the material map, then run the single-file guard.
	sfa.srcFileToEncryptionMaterial = make(map[string]*snowflakeFileEncryption)
	for i, f := range sfa.srcFiles {
		sfa.srcFileToEncryptionMaterial[f] = sfa.encryptionMaterial[i]
	}
	assertNilF(t, sfa.ensureSingleGetStreamFile())

	assertNilF(t, sfa.initFileMetadata())
	assertEqualE(t, len(sfa.fileMetadata), 1)
	assertEqualE(t, sfa.fileMetadata[0].encryptionMaterial, m0)
}
