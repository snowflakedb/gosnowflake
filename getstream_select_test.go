package gosnowflake

import "testing"

// getCmd builds a GET command whose stage source is src (quoted, as the driver receives it).
func getCmd(src string) string {
	return "GET '" + src + "' 'file:///tmp/dest'"
}

// TestGetStageSourceFromCommand covers parsing the requested stage source out of a GET command.
func TestGetStageSourceFromCommand(t *testing.T) {
	sfa := &snowflakeFileTransferAgent{}
	tests := []struct {
		command string
		want    string
	}{
		{"GET 'snow://w/live/foo' 'file:///tmp'", "snow://w/live/foo"},
		{"GET @db.sc.stg/dir/foo.txt file:///tmp parallel=10", "db.sc.stg/dir/foo.txt"},
		{"GET '@stage/dir/foo'", "stage/dir/foo"},
		{"get @~/data1.txt.gz file:///tmp/testData", "~/data1.txt.gz"},
		{"/* c */ GET 'snow://w/foo' 'file:///x'", "snow://w/foo"},
		{"GET 'snow://w/live/' 'file:///tmp'", "snow://w/live/"},
		{"SELECT 1", ""},
		{"GET ", ""},
		{"GET '@stage/unclosed file:///tmp", ""},
	}
	for _, tt := range tests {
		if got := sfa.getStageSourceFromCommand(tt.command); got != tt.want {
			t.Errorf("getStageSourceFromCommand(%q) = %q, want %q", tt.command, got, tt.want)
		}
	}

	// The requested file is the leaf of the source; spot-check baseName extraction.
	if got := baseName(sfa.getStageSourceFromCommand("GET 'snow://w/live/foo' 'file:///tmp'")); got != "foo" {
		t.Errorf("leaf = %q, want foo", got)
	}
	if got := baseName(sfa.getStageSourceFromCommand("GET 'snow://w/live/' 'file:///tmp'")); got != "" {
		t.Errorf("bare-prefix leaf = %q, want empty", got)
	}
}

// TestSelectGetStreamFile exercises the default get-stream resolution: the GET is narrowed to
// the single file it requested, or an error is returned. No live connection is needed - the
// method only reads command/srcFiles/data, and exceptionTelemetry tolerates a nil conn.
//
// Realistic workspace (FBE) physical paths look like versions/<entity>/<versionId>/<logical>,
// with a per-file <versionId> that differs between files.
func TestSelectGetStreamFile(t *testing.T) {
	ws := "versions/29_559629944250378"
	v1 := ws + "/1782146627271"
	v2 := ws + "/1782164111378"
	v3 := ws + "/1782146628397"

	tests := []struct {
		name       string
		cmdSrc     string // stage source in the GET command
		srcFiles   []string
		wantFiles  []string // expected srcFiles on success (nil when an error is expected)
		wantErrNum int      // 0 when no error expected
	}{
		{
			name:      "single result is streamed",
			cmdSrc:    "snow://w/live/report.csv",
			srcFiles:  []string{v1 + "/report.csv"},
			wantFiles: []string{v1 + "/report.csv"},
		},
		{
			name:      "prefix-sibling resolved to the requested leaf (foo not foobar)",
			cmdSrc:    "snow://w/live/foo",
			srcFiles:  []string{v1 + "/foo", v2 + "/foobar"},
			wantFiles: []string{v1 + "/foo"},
		},
		{
			name:      "the longer sibling resolves when it is the one requested",
			cmdSrc:    "snow://w/live/foobar",
			srcFiles:  []string{v1 + "/foo", v2 + "/foobar"},
			wantFiles: []string{v2 + "/foobar"},
		},
		{
			name:      "dotted suffix sibling (profiles.yml vs profiles.yml.template)",
			cmdSrc:    "snow://w/live/profiles.yml",
			srcFiles:  []string{v1 + "/profiles.yml", v2 + "/profiles.yml.template"},
			wantFiles: []string{v1 + "/profiles.yml"},
		},
		{
			// A top-level "foo" and a nested "foo/foo" both have leaf "foo": ambiguous,
			// never guess.
			name:       "file and same-named subdir are ambiguous",
			cmdSrc:     "snow://w/live/foo",
			srcFiles:   []string{v1 + "/foo", v2 + "/foo/foo"},
			wantErrNum: ErrGetStreamMultipleFiles,
		},
		{
			name:       "same leaf in different directories is ambiguous",
			cmdSrc:     "snow://w/live/foo",
			srcFiles:   []string{v1 + "/a/foo", v2 + "/b/foo"},
			wantErrNum: ErrGetStreamMultipleFiles,
		},
		{
			// A single prefix-sibling (requested file absent) streams: with one result the
			// driver can't distinguish this from a folder GET that resolves to one file, so it
			// streams rather than guessing the file is "missing".
			name:      "single prefix-sibling is streamed (no N==1 validation)",
			cmdSrc:    "snow://w/live/foo",
			srcFiles:  []string{v1 + "/foobar"},
			wantFiles: []string{v1 + "/foobar"},
		},
		{
			name:       "no exact match among multiple results is ambiguous",
			cmdSrc:     "snow://w/live/fo",
			srcFiles:   []string{v1 + "/foo", v2 + "/foobar"},
			wantErrNum: ErrGetStreamMultipleFiles,
		},
		{
			// Whole-stage / table-stage GET: the source names no file, so the leaf matches
			// nothing; a single result still streams (regression test for stage/folder GETs).
			name:      "whole-stage GET streams its single file",
			cmdSrc:    "@%mytable",
			srcFiles:  []string{v1 + "/data.csv.gz"},
			wantFiles: []string{v1 + "/data.csv.gz"},
		},
		{
			name:       "whole-stage GET with multiple files is ambiguous",
			cmdSrc:     "@%mytable",
			srcFiles:   []string{v1 + "/a.csv.gz", v2 + "/b.csv.gz"},
			wantErrNum: ErrGetStreamMultipleFiles,
		},
		{
			// Folder GET: the source leaf is a directory name that matches no result file; a
			// single result still streams.
			name:      "folder GET streams its single file",
			cmdSrc:    "snow://w/live/somedir",
			srcFiles:  []string{v1 + "/somedir/data.csv"},
			wantFiles: []string{v1 + "/somedir/data.csv"},
		},
		{
			name:       "bare-prefix GET with multiple results is ambiguous",
			cmdSrc:     "snow://w/live/",
			srcFiles:   []string{v1 + "/a", v2 + "/b"},
			wantErrNum: ErrGetStreamMultipleFiles,
		},
		{
			name:      "bare-prefix GET with a single result streams it",
			cmdSrc:    "snow://w/live/",
			srcFiles:  []string{v1 + "/a"},
			wantFiles: []string{v1 + "/a"},
		},
		{
			name:      "matching is case-sensitive when resolving among results",
			cmdSrc:    "snow://w/live/Foo",
			srcFiles:  []string{v1 + "/foo", v2 + "/Foo"},
			wantFiles: []string{v2 + "/Foo"},
		},
		{
			name:      "shared prefix across three files resolves the exact leaf",
			cmdSrc:    "snow://w/live/foo",
			srcFiles:  []string{v1 + "/foo", v2 + "/foobar", v3 + "/foobaz"},
			wantFiles: []string{v1 + "/foo"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sfa := &snowflakeFileTransferAgent{
				commandType: downloadCommand,
				command:     getCmd(tt.cmdSrc),
				srcFiles:    tt.srcFiles,
				data:        &execResponseData{},
			}

			err := sfa.selectGetStreamFile()

			if tt.wantErrNum != 0 {
				sfErr, ok := err.(*SnowflakeError)
				if !ok {
					t.Fatalf("expected *SnowflakeError %d, got %T: %v", tt.wantErrNum, err, err)
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

// TestSelectGetStreamFileLoneDeeperNamesake documents a KNOWN, UNDESIRED limitation.
//
// When the only result for request "foo" is a deeper namesake ".../foo/foo" (leaf "foo"), it is
// streamed instead of erroring. From one physical path the driver cannot tell the logical name
// is "foo/foo" rather than "foo". The *correct* outcome would be ErrFileNotExists; full
// correctness requires the GET to resolve to the exact object server-side. This test pins the
// current limited behavior so any change to it is a conscious decision.
func TestSelectGetStreamFileLoneDeeperNamesake(t *testing.T) {
	const loneNested = "versions/29_559629944250378/1782146627271/foo/foo"
	sfa := &snowflakeFileTransferAgent{
		commandType: downloadCommand,
		command:     getCmd("snow://w/live/foo"),
		srcFiles:    []string{loneNested},
		data:        &execResponseData{},
	}

	if err := sfa.selectGetStreamFile(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// LIMITATION: ideally ErrFileNotExists (no top-level "foo"); instead the deeper namesake
	// "foo/foo" is wrongly accepted for a request of "foo".
	if !equalStringSlice(sfa.srcFiles, []string{loneNested}) {
		t.Fatalf("srcFiles = %v, want [%s] (documenting current limited behavior)", sfa.srcFiles, loneNested)
	}
}

// TestSelectGetStreamFileEncryptionMaterialAlignment guards the ordering bug: selection must
// run AFTER the source-file -> encryption-material map is built, so a selected file at index > 0
// keeps its own material rather than the first file's.
func TestSelectGetStreamFileEncryptionMaterialAlignment(t *testing.T) {
	m0 := &snowflakeFileEncryption{SMKID: 100, QueryID: "q0"}
	m1 := &snowflakeFileEncryption{SMKID: 101, QueryID: "q1"}
	m2 := &snowflakeFileEncryption{SMKID: 102, QueryID: "q2"}
	ws := "versions/29_559629944250378"
	srcFiles := []string{ws + "/1782146627271/alpha", ws + "/1782164111378/bravo", ws + "/1782146628397/charlie"}

	sfa := &snowflakeFileTransferAgent{
		commandType:        downloadCommand,
		command:            getCmd("snow://w/live/bravo"),
		srcFiles:           srcFiles,
		encryptionMaterial: []*snowflakeFileEncryption{m0, m1, m2},
		data:               &execResponseData{SrcLocations: srcFiles},
	}

	// Mirror parseCommand: build the material map over the FULL result set first...
	sfa.srcFileToEncryptionMaterial = make(map[string]*snowflakeFileEncryption)
	for i, f := range sfa.srcFiles {
		sfa.srcFileToEncryptionMaterial[f] = sfa.encryptionMaterial[i]
	}
	// ...then resolve to the requested file.
	if err := sfa.selectGetStreamFile(); err != nil {
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
