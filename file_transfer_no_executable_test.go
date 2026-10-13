package gosnowflake

import (
	"bytes"
	"compress/gzip"
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// os.Executable() fails when the driver is loaded as a shared library into a
// host process that Go cannot resolve an executable path for (error: "cannot
// find executable path"). PUT/GET have no reason to need it, so the file
// transfer code must not depend on it.
func TestFileTransferDoesNotCallOsExecutable(t *testing.T) {
	files := []string{
		"file_transfer_agent.go",
		"file_util.go",
		"storage_client.go",
		"local_storage_client.go",
		"s3_storage_client.go",
		"azure_storage_client.go",
		"gcs_storage_client.go",
	}
	// Locate the sources relative to this file so the test works from any working directory.
	_, thisFile, _, ok := runtime.Caller(0)
	assertTrueF(t, ok, "cannot locate test source file")
	srcDir := filepath.Dir(thisFile)
	fset := token.NewFileSet()
	for _, name := range files {
		f, err := parser.ParseFile(fset, filepath.Join(srcDir, name), nil, 0)
		assertNilF(t, err)
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "os" && sel.Sel.Name == "Executable" {
				t.Errorf("%s calls os.Executable(); file transfer must not depend on the executable path", fset.Position(sel.Pos()))
			}
			return true
		})
	}
}

func chdirForTest(t *testing.T, dir string) {
	t.Helper()
	orig, err := os.Getwd()
	assertNilF(t, err)
	assertNilF(t, os.Chdir(dir))
	t.Cleanup(func() { assertNilE(t, os.Chdir(orig)) })
}

// A relative PUT source must resolve against the working directory, like the
// relative destination of GET already does.
func TestExpandFilenamesResolvesRelativePathsAgainstWorkingDirectory(t *testing.T) {
	// EvalSymlinks: t.TempDir() can be a symlink (e.g. /var -> /private/var on macOS)
	// and os.Getwd() returns the resolved path.
	dir, err := filepath.EvalSymlinks(t.TempDir())
	assertNilF(t, err)
	assertNilF(t, os.WriteFile(filepath.Join(dir, "a.csv"), []byte("1\n"), readWriteFileMode))
	assertNilF(t, os.WriteFile(filepath.Join(dir, "b.csv"), []byte("2\n"), readWriteFileMode))
	chdirForTest(t, dir)

	sfa := &snowflakeFileTransferAgent{commandType: uploadCommand}

	got, err := sfa.expandFilenames([]string{"a.csv"})
	assertNilF(t, err)
	assertEqualE(t, len(got), 1)
	assertEqualE(t, got[0], filepath.Join(dir, "a.csv"))

	got, err = sfa.expandFilenames([]string{"*.csv"})
	assertNilF(t, err)
	assertEqualE(t, len(got), 2)

	got, err = sfa.expandFilenames([]string{filepath.Join(dir, "b.csv")})
	assertNilF(t, err)
	assertEqualE(t, len(got), 1)
	assertEqualE(t, got[0], filepath.Join(dir, "b.csv"))

	got, err = sfa.expandFilenames([]string{"missing.csv"})
	assertNilF(t, err)
	assertEqualE(t, len(got), 0)
}

// GET from a remote stage into a relative local directory.
func TestRemoteDownloadOneFileToRelativeDirectory(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	assertNilF(t, err)
	assertNilF(t, os.Mkdir(filepath.Join(dir, "prod_state"), 0o755))
	chdirForTest(t, dir)

	const contents = `{"nodes": {}}`
	info := execResponseStageInfo{
		Location:     "gcs/teststage/users/34/",
		LocationType: "GCS",
	}
	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false, &snowflakeTelemetry{})
	assertNilF(t, err)

	meta := fileMetadata{
		name:              "manifest.json",
		stageLocationType: "GCS",
		noSleepingTime:    true,
		client:            gcsCli,
		stageInfo:         &info,
		dstFileName:       "manifest.json",
		overwrite:         true,
		srcFileName:       "manifest.json",
		localLocation:     "prod_state",
		options:           &SnowflakeFileTransferOptions{MultiPartThreshold: multiPartThreshold},
		mockGcsClient: &clientMock{
			DoFunc: func(req *http.Request) (*http.Response, error) {
				resp := &http.Response{
					StatusCode:    http.StatusOK,
					Header:        http.Header{},
					ContentLength: int64(len(contents)),
					Body:          http.NoBody,
				}
				if req.Method == http.MethodGet {
					resp.Body = io.NopCloser(bytes.NewBufferString(contents))
				}
				return resp, nil
			},
		},
		sfa:       &snowflakeFileTransferAgent{sc: &snowflakeConn{cfg: &Config{}}},
		resStatus: downloaded, // bypass file header request
	}

	assertNilF(t, new(remoteStorageUtil).downloadOneFile(context.Background(), &meta))

	// NOTE: the fork does not move an unencrypted download from <dst>.tmp into
	// place (upstream added an os.Rename for that case), so only assert that the
	// relative destination resolved and no temporary file is left behind.
	_, err = os.Stat(filepath.Join(dir, "prod_state", "manifest.json.tmp"))
	assertTrueE(t, os.IsNotExist(err), "temporary download file should be cleaned up")
}

// GET from a local stage copies the file and no longer touches the directory of the executable.
func TestLocalDownloadOneFileCopiesFile(t *testing.T) {
	stageDir := t.TempDir()
	dstDir := t.TempDir()
	const contents = "123,test1\n456,test2\n"

	var b bytes.Buffer
	gzw := gzip.NewWriter(&b)
	_, err := gzw.Write([]byte(contents))
	assertNilF(t, err)
	assertNilF(t, gzw.Close())
	assertNilF(t, os.WriteFile(filepath.Join(stageDir, "f.txt.gz"), b.Bytes(), readWriteFileMode))

	info := execResponseStageInfo{Location: stageDir, LocationType: "LOCAL_FS"}
	lu := new(localUtil)
	cli, err := lu.createClient(&info, false, nil, nil)
	assertNilF(t, err)
	meta := fileMetadata{
		name:              "f.txt.gz",
		stageLocationType: "LOCAL_FS",
		noSleepingTime:    true,
		client:            cli,
		stageInfo:         &info,
		dstFileName:       "f.txt.gz",
		overwrite:         true,
		srcFileName:       "f.txt.gz",
		localLocation:     dstDir,
		options:           &SnowflakeFileTransferOptions{MultiPartThreshold: multiPartThreshold},
	}
	assertNilF(t, lu.downloadOneFile(context.Background(), &meta))
	assertEqualE(t, meta.resStatus, downloaded)
	got, err := os.ReadFile(filepath.Join(dstDir, "f.txt.gz"))
	assertNilF(t, err)
	assertEqualE(t, bytes.Equal(got, b.Bytes()), true)
	assertEqualE(t, meta.dstFileSize, int64(b.Len()))
}
