package gosnowflake

import (
	"bytes"
	"context"
	"testing"
)

// TestGetFileGetStreamExactFile verifies the opt-in API sets a retrievable target,
// that the plain get-stream constructor reports no target, and that an explicit empty
// name is reported as present-but-empty (a misuse the selector rejects).
func TestGetFileGetStreamExactFile(t *testing.T) {
	var buf bytes.Buffer

	if v, ok := getFileGetStreamExactFile(WithFileGetStream(context.Background(), &buf)); ok {
		t.Fatalf("plain WithFileGetStream should report no target, got %q ok=true", v)
	}
	if v, ok := getFileGetStreamExactFile(WithFileGetStreamForExactFile(context.Background(), &buf, "dir/foo")); !ok || v != "dir/foo" {
		t.Fatalf("target = %q ok=%v, want %q true", v, ok, "dir/foo")
	}
	if v, ok := getFileGetStreamExactFile(WithFileGetStreamForExactFile(context.Background(), &buf, "")); !ok || v != "" {
		t.Fatalf("explicit empty target should be present with empty value, got %q ok=%v", v, ok)
	}
	if !isFileGetStream(WithFileGetStreamForExactFile(context.Background(), &buf, "foo")) {
		t.Fatal("WithFileGetStreamForExactFile must also mark the context as a get-stream")
	}
}

// TestSelectGetStreamExactFile exercises the single-file selection that protects
// the get-stream primitive from the N>1 prefix-match case. No live connection is needed:
// the method only reads ctx/srcFiles/data, and exceptionTelemetry tolerates a nil conn.
func TestSelectGetStreamExactFile(t *testing.T) {
	// Realistic workspace (FBE) physical paths look like versions/<entity>/<versionId>/<logical>.
	// The <versionId> segment differs per file (it is a per-file timestamp), so selection
	// compares depth *relative* to the other candidates and never needs to locate where the
	// physical prefix ends.
	ws := "versions/29_559629944250378"
	v1 := ws + "/1782146627271"
	v2 := ws + "/1782164111378"
	v3 := ws + "/1782146628397"

	tests := []struct {
		name       string
		target     string // "" means plain WithFileGetStream (no opt-in)
		srcFiles   []string
		wantFiles  []string // expected srcFiles on success (nil when an error is expected)
		wantErrNum int      // 0 when no error expected
	}{
		{
			name:      "plain get-stream is untouched even with multiple matches",
			target:    "",
			srcFiles:  []string{v1 + "/foo", v2 + "/foobar"},
			wantFiles: []string{v1 + "/foo", v2 + "/foobar"},
		},
		{
			name:      "single result unchanged",
			target:    "report.csv",
			srcFiles:  []string{v1 + "/report.csv"},
			wantFiles: []string{v1 + "/report.csv"},
		},
		{
			name:      "shared prefix selects the exact leaf (foo not foobar)",
			target:    "foo",
			srcFiles:  []string{v1 + "/foo", v2 + "/foobar"},
			wantFiles: []string{v1 + "/foo"},
		},
		{
			name:      "selects the longer sibling when it is the one requested",
			target:    "foobar",
			srcFiles:  []string{v1 + "/foo", v2 + "/foobar"},
			wantFiles: []string{v2 + "/foobar"},
		},
		{
			name:      "dotted suffix sibling (profiles.yml vs profiles.yml.template)",
			target:    "profiles.yml",
			srcFiles:  []string{v1 + "/profiles.yml", v2 + "/profiles.yml.template"},
			wantFiles: []string{v1 + "/profiles.yml"},
		},
		{
			// The key relative-depth case: a top-level "foo" and a nested "foo/foo" both
			// end in leaf "foo", but "foo" is shallower (different version ids, same prefix
			// depth), so it is the exact file and wins - no error.
			name:      "top-level file wins over a deeper same-leaf namesake",
			target:    "foo",
			srcFiles:  []string{v1 + "/foo", v2 + "/foo/foo"},
			wantFiles: []string{v1 + "/foo"},
		},
		{
			name:      "shallowest wins across three depths",
			target:    "foo",
			srcFiles:  []string{v1 + "/foo", v2 + "/foo/foo", v3 + "/foo/bar/foo"},
			wantFiles: []string{v1 + "/foo"},
		},
		{
			// No top-level namesake exists; the unique shallowest is still returned rather
			// than erroring (shortest-wins is a better outcome than throwing).
			name:      "unique shallowest namesake wins when none is top-level",
			target:    "config.yml",
			srcFiles:  []string{v1 + "/envs/config.yml", v2 + "/envs/staging/config.yml"},
			wantFiles: []string{v1 + "/envs/config.yml"},
		},
		{
			name:      "nested path explicitly selects the file inside the same-named dir",
			target:    "foo/foo",
			srcFiles:  []string{v1 + "/foo", v2 + "/foo/foo"},
			wantFiles: []string{v2 + "/foo/foo"},
		},
		{
			// The core fail-loud case: requested file does not exist, but a single
			// prefix-sibling does. Must NOT silently stream the sibling.
			name:       "no exact match returns ErrFileNotExists (single sibling present)",
			target:     "foo",
			srcFiles:   []string{v1 + "/foobar"},
			wantErrNum: ErrFileNotExists,
		},
		{
			name:       "no exact match returns ErrFileNotExists (multiple siblings)",
			target:     "foo",
			srcFiles:   []string{v1 + "/foobar", v2 + "/foobaz"},
			wantErrNum: ErrFileNotExists,
		},
		{
			// Equally-shallow same-leaf files in different directories: genuinely ambiguous,
			// no shallower exact candidate exists.
			name:       "equally-shallow namesakes are ambiguous",
			target:     "config.yml",
			srcFiles:   []string{v1 + "/dev/config.yml", v2 + "/prod/config.yml"},
			wantErrNum: ErrGetStreamMultipleFiles,
		},
		{
			name:      "fuller path disambiguates equally-shallow namesakes",
			target:    "dev/config.yml",
			srcFiles:  []string{v1 + "/dev/config.yml", v2 + "/prod/config.yml"},
			wantFiles: []string{v1 + "/dev/config.yml"},
		},
		{
			// Two files tie at the shallowest depth while a third is deeper: still ambiguous
			// (the deeper one does not break the tie).
			name:       "tie at the shallowest depth is ambiguous even with a deeper match",
			target:     "config.yml",
			srcFiles:   []string{v1 + "/dev/config.yml", v2 + "/prod/config.yml", v3 + "/dev/old/config.yml"},
			wantErrNum: ErrGetStreamMultipleFiles,
		},
		{
			name:       "matching is case-sensitive",
			target:     "Foo",
			srcFiles:   []string{v1 + "/foo"},
			wantErrNum: ErrFileNotExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			ctx := WithFileGetStream(context.Background(), &buf)
			if tt.target != "" {
				ctx = WithFileGetStreamForExactFile(context.Background(), &buf, tt.target)
			}
			sfa := &snowflakeFileTransferAgent{
				ctx:         ctx,
				commandType: downloadCommand,
				srcFiles:    tt.srcFiles,
				data:        &execResponseData{},
			}

			err := sfa.selectGetStreamExactFile()

			if tt.wantErrNum != 0 {
				if err == nil {
					t.Fatalf("expected error %d, got nil", tt.wantErrNum)
				}
				sfErr, ok := err.(*SnowflakeError)
				if !ok {
					t.Fatalf("expected *SnowflakeError, got %T: %v", err, err)
				}
				if sfErr.Number != tt.wantErrNum {
					t.Fatalf("error number = %d, want %d", sfErr.Number, tt.wantErrNum)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !equalStringSlice(sfa.srcFiles, tt.wantFiles) {
				t.Fatalf("srcFiles = %v, want %v", sfa.srcFiles, tt.wantFiles)
			}
		})
	}
}

// TestSelectGetStreamExactFileEmptyName verifies that opting into exact-file
// streaming with an empty name fails loud rather than silently falling back to the
// multi-file path.
func TestSelectGetStreamExactFileEmptyName(t *testing.T) {
	var buf bytes.Buffer
	sfa := &snowflakeFileTransferAgent{
		ctx:         WithFileGetStreamForExactFile(context.Background(), &buf, ""),
		commandType: downloadCommand,
		srcFiles:    []string{"versions/29_559629944250378/1782146627271/foo", "versions/29_559629944250378/1782164111378/foobar"},
		data:        &execResponseData{},
	}

	err := sfa.selectGetStreamExactFile()
	sfErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("expected *SnowflakeError, got %T: %v", err, err)
	}
	if sfErr.Number != ErrFileNotExists {
		t.Fatalf("error number = %d, want %d", sfErr.Number, ErrFileNotExists)
	}
	if !equalStringSlice(sfa.srcFiles, []string{"versions/29_559629944250378/1782146627271/foo", "versions/29_559629944250378/1782164111378/foobar"}) {
		t.Fatalf("srcFiles must be untouched on error, got %v", sfa.srcFiles)
	}
}

// TestSelectGetStreamExactFileLoneDeeperNamesake documents a KNOWN, UNDESIRED limitation.
//
// When the GET resolves to a single deeper namesake - request "foo", but the result contains
// only ".../foo/foo" with no top-level "foo" - the driver returns that file instead of erroring.
// From one physical path it cannot tell the logical name is "foo/foo" rather than "foo" (the
// per-file version-id prefix has no fixed boundary), and the relative-depth tiebreak needs >=2
// candidates to detect over-depth. The *correct* outcome would be ErrFileNotExists; full
// correctness requires the GET to resolve to the exact object server-side (the driver selection
// is a backstop for the multi-file race, not a logical-identity oracle).
//
// This test pins the current limited behavior so that any change to it is a conscious decision.
func TestSelectGetStreamExactFileLoneDeeperNamesake(t *testing.T) {
	const loneNested = "versions/29_559629944250378/1782146627271/foo/foo"
	var buf bytes.Buffer
	sfa := &snowflakeFileTransferAgent{
		ctx:         WithFileGetStreamForExactFile(context.Background(), &buf, "foo"),
		commandType: downloadCommand,
		srcFiles:    []string{loneNested},
		data:        &execResponseData{},
	}

	if err := sfa.selectGetStreamExactFile(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// LIMITATION: ideally this would error with ErrFileNotExists (no top-level "foo" exists);
	// instead the deeper namesake "foo/foo" is wrongly accepted for a request of "foo".
	if !equalStringSlice(sfa.srcFiles, []string{loneNested}) {
		t.Fatalf("srcFiles = %v, want [%s] (documenting current limited behavior)", sfa.srcFiles, loneNested)
	}
}

// TestSelectGetStreamExactFileEncryptionMaterialAlignment guards the ordering bug:
// selection must run AFTER the source-file -> encryption-material map is built, so a
// selected file at index > 0 keeps its own material rather than the first file's.
func TestSelectGetStreamExactFileEncryptionMaterialAlignment(t *testing.T) {
	m0 := &snowflakeFileEncryption{SMKID: 100, QueryID: "q0"}
	m1 := &snowflakeFileEncryption{SMKID: 101, QueryID: "q1"}
	m2 := &snowflakeFileEncryption{SMKID: 102, QueryID: "q2"}
	ws := "versions/29_559629944250378"
	// Different version ids per file, just like the real GET response.
	srcFiles := []string{ws + "/1782146627271/alpha", ws + "/1782164111378/bravo", ws + "/1782146628397/charlie"}

	var buf bytes.Buffer
	sfa := &snowflakeFileTransferAgent{
		ctx:                WithFileGetStreamForExactFile(context.Background(), &buf, "bravo"),
		commandType:        downloadCommand,
		srcFiles:           srcFiles,
		encryptionMaterial: []*snowflakeFileEncryption{m0, m1, m2},
		data:               &execResponseData{SrcLocations: srcFiles},
	}

	// Mirror parseCommand: build the material map over the FULL result set first...
	sfa.srcFileToEncryptionMaterial = make(map[string]*snowflakeFileEncryption)
	for i, f := range sfa.srcFiles {
		sfa.srcFileToEncryptionMaterial[f] = sfa.encryptionMaterial[i]
	}
	// ...then narrow to the requested file.
	if err := sfa.selectGetStreamExactFile(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sfa.srcFiles) != 1 || sfa.srcFiles[0] != ws+"/1782164111378/bravo" {
		t.Fatalf("srcFiles = %v, want [%s/1782164111378/bravo]", sfa.srcFiles, ws)
	}

	if err := sfa.initFileMetadata(); err != nil {
		t.Fatalf("initFileMetadata: %v", err)
	}
	if len(sfa.fileMetadata) != 1 {
		t.Fatalf("fileMetadata count = %d, want 1", len(sfa.fileMetadata))
	}
	if got := sfa.fileMetadata[0].encryptionMaterial; got != m1 {
		t.Fatalf("selected file got material %+v, want m1 (SMKID 101) - index alignment broke", got)
	}
}

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
