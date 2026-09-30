package gosnowflake

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildStageFileRef(t *testing.T) {
	assertEqualF(t, buildStageFileRef("@stage", "file.csv"), "@stage/file.csv")
	assertEqualF(t, buildStageFileRef("@stage", "/file.csv"), "@stage/file.csv")
	assertEqualF(t, buildStageFileRef("stage", "dir/file.csv"), "@stage/dir/file.csv")
	assertEqualF(t, buildStageFileRef("'@%\"ice cream (nice)\"'", "hello.txt"), `@%"ice cream (nice)"/hello.txt`)
}

func TestQuoteStageRefIfNeeded(t *testing.T) {
	assertEqualF(t, quoteStageRefIfNeeded("@stage"), "@stage")
	assertEqualF(t, quoteStageRefIfNeeded("@stage/dir/file.csv"), "@stage/dir/file.csv")
	assertEqualF(t, quoteStageRefIfNeeded(`@"DB"."SCHEMA".STAGE`), `@"DB"."SCHEMA".STAGE`)
	quoted := quoteStageRefIfNeeded(`@"DB"."日本語".STAGE`)
	assertEqualF(t, quoted, `'@"DB"."日本語".STAGE'`)
	assertTrueF(t, needsStageQuoting(`@"DQ"."自動化専用_変更禁止".SNOWPARK_TEMP_STAGE_XYZ/file.txt`))
	assertFalseF(t, needsStageQuoting("'@already/quoted'"))
}

func TestBuildDownloadStreamGetSQL(t *testing.T) {
	sql := buildDownloadStreamGetSQL("@~", "data.csv")
	assertEqualF(t, sql, "GET @~/data.csv "+downloadStreamDummyGetPath)
}

func TestMatchDownloadStreamFile(t *testing.T) {
	got, err := matchDownloadStreamFile([]string{"path/data.csv"}, "data.csv")
	assertNilF(t, err)
	assertEqualF(t, got, "path/data.csv")

	got, err = matchDownloadStreamFile([]string{"path/data.csv.gz"}, "data.csv")
	assertNilF(t, err)
	assertEqualF(t, got, "path/data.csv.gz")

	_, err = matchDownloadStreamFile([]string{"a/foo.csv", "b/foo.csv"}, "foo.csv")
	assertNotNilF(t, err)
	var se *SnowflakeError
	assertErrorsAsF(t, err, &se)
	assertEqualF(t, se.Number, ErrGetStreamMultipleFiles)

	_, err = matchDownloadStreamFile(nil, "missing.csv")
	assertNotNilF(t, err)
	assertErrorsAsF(t, err, &se)
	assertEqualF(t, se.Number, ErrFileNotExists)
	assertEqualF(t, se.SQLState, SQLStateNoData)
}

func TestDownloadStreamRejectsEmptyArgs(t *testing.T) {
	sc := &snowflakeConn{}
	_, err := sc.DownloadStream(context.Background(), "", "file.csv")
	assertNotNilF(t, err)
	var se *SnowflakeError
	assertErrorsAsF(t, err, &se)
	assertEqualF(t, se.Number, ErrDownloadStreamInvalidArg)

	_, err = sc.DownloadStream(context.Background(), "@~", "  ")
	assertNotNilF(t, err)
	assertErrorsAsF(t, err, &se)
	assertEqualF(t, se.Number, ErrDownloadStreamInvalidArg)
}

func TestMaybeDecryptStreamRoundTrip(t *testing.T) {
	encMat := snowflakeFileEncryption{
		QueryStageMasterKey: "ztke8tIdVt1zmlQIZm0BMA==",
		QueryID:             "123873c7-3a66-40c4-ab89-e3722fbccce1",
		SMKID:               9223372036854775807,
	}
	plain := []byte("download stream decrypt payload")
	var encrypted bytes.Buffer
	meta, err := encryptStreamCBC(&encMat, bytes.NewReader(plain), &encrypted, 0)
	assertNilF(t, err)
	assertNotNilF(t, meta)

	rc, err := maybeDecryptStream(io.NopCloser(bytes.NewReader(encrypted.Bytes())), &fileHeader{encryptionMetadata: meta}, &encMat)
	assertNilF(t, err)
	got, err := io.ReadAll(rc)
	assertNilF(t, err)
	assertNilF(t, rc.Close())
	assertEqualF(t, string(got), string(plain))
}

func TestMaybeDecryptStreamPassthrough(t *testing.T) {
	payload := []byte("unencrypted")
	rc, err := maybeDecryptStream(io.NopCloser(bytes.NewReader(payload)), &fileHeader{}, nil)
	assertNilF(t, err)
	got, err := io.ReadAll(rc)
	assertNilF(t, err)
	assertEqualF(t, string(got), string(payload))
}

type trackingCloser struct {
	io.Reader
	closed int
}

func (c *trackingCloser) Close() error {
	c.closed++
	return nil
}

func TestMaybeDecryptStreamFailClosed(t *testing.T) {
	enc := &snowflakeFileEncryption{QueryStageMasterKey: "YWJjZGVmMTIzNDU2Nzg5MA=="}
	body := &trackingCloser{Reader: bytes.NewReader([]byte("ciphertext"))}
	_, err := maybeDecryptStream(body, &fileHeader{}, enc)
	assertNotNilF(t, err)
	var se *SnowflakeError
	assertErrorsAsF(t, err, &se)
	assertEqualF(t, se.Number, ErrFailedToDecrypt)
	assertEqualF(t, body.closed, 1)

	body = &trackingCloser{Reader: bytes.NewReader([]byte("ciphertext"))}
	_, err = maybeDecryptStream(body, &fileHeader{encryptionMetadata: &encryptMetadata{}}, enc)
	assertNotNilF(t, err)
	assertErrorsAsF(t, err, &se)
	assertEqualF(t, se.Number, ErrFailedToDecrypt)
	assertEqualF(t, body.closed, 1)
}

func TestOwnedReadCloserCloseIsIdempotentAndCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	body := &trackingCloser{Reader: bytes.NewReader([]byte("x"))}
	owned := &ownedReadCloser{Reader: body, closer: body, cancel: cancel}
	assertNilF(t, owned.Close())
	assertNilF(t, owned.Close())
	assertEqualF(t, body.closed, 1)
	select {
	case <-ctx.Done():
	default:
		assertTrueF(t, false, "expected body context to be canceled")
	}
}

func TestOpenDownloadStreamLocalFS(t *testing.T) {
	sfa := &snowflakeFileTransferAgent{
		ctx:                    context.Background(),
		streamDownload:         true,
		streamDownloadFileName: "f.txt",
		data: &execResponseData{
			Command:      string(downloadCommand),
			SrcLocations: []string{"f.txt"},
			StageInfo:    execResponseStageInfo{LocationType: "LOCAL_FS"},
		},
		sc: &snowflakeConn{cfg: &Config{}},
	}
	_, err := sfa.openDownloadStream(context.Background(), "f.txt")
	assertNotNilF(t, err)
	var se *SnowflakeError
	assertErrorsAsF(t, err, &se)
	assertEqualF(t, se.Number, ErrDownloadStreamLocalFS)
}

func TestWrapGzipDownloadStreamInvalidHeader(t *testing.T) {
	body := &trackingCloser{Reader: bytes.NewReader([]byte("not gzip"))}
	owned := &ownedReadCloser{Reader: body, closer: body}
	r := wrapGzipDownloadStream(&snowflakeConn{}, owned)
	assertNotNilF(t, r)
	buf := make([]byte, 8)
	n, err := r.Read(buf)
	assertEqualF(t, n, 0)
	assertNotNilF(t, err)
	var se *SnowflakeError
	assertErrorsAsF(t, err, &se)
	assertEqualF(t, se.Number, ErrDownloadStreamDecompress)
	assertNilF(t, r.Close())
	assertNilF(t, r.Close())
	assertEqualF(t, body.closed, 1)
}

type errAfterReader struct {
	data []byte
	err  error
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}

func TestWrapGzipDownloadStreamPreservesUnderlyingError(t *testing.T) {
	body := &trackingCloser{Reader: &errAfterReader{err: context.Canceled}}
	owned := &ownedReadCloser{Reader: body, closer: body}
	r := wrapGzipDownloadStream(&snowflakeConn{}, owned)
	_, err := r.Read(make([]byte, 8))
	assertTrueF(t, errors.Is(err, context.Canceled), fmt.Sprintf("expected context.Canceled, got %v", err))
	var se *SnowflakeError
	mapped := errors.As(err, &se) && se.Number == ErrDownloadStreamDecompress
	assertFalseF(t, mapped, "transport errors must not become ErrDownloadStreamDecompress")
	assertNilF(t, r.Close())

	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, err = zw.Write([]byte("hello"))
	assertNilF(t, err)
	assertNilF(t, zw.Close())
	body = &trackingCloser{Reader: &errAfterReader{data: compressed.Bytes(), err: context.DeadlineExceeded}}
	owned = &ownedReadCloser{Reader: body, closer: body}
	r = wrapGzipDownloadStream(&snowflakeConn{}, owned)
	_, err = io.ReadAll(r)
	assertTrueF(t, errors.Is(err, context.DeadlineExceeded), fmt.Sprintf("expected context.DeadlineExceeded, got %v", err))
	se = nil
	mapped = errors.As(err, &se) && se.Number == ErrDownloadStreamDecompress
	assertFalseF(t, mapped, "deadline errors must not become ErrDownloadStreamDecompress")
	assertNilF(t, r.Close())
}

func TestOwnedReadCloserCloseBeforeEOF(t *testing.T) {
	pr, pw := io.Pipe()
	owned := &ownedReadCloser{Reader: pr, closer: pr}
	done := make(chan error, 1)
	go func() {
		done <- owned.Close()
	}()
	select {
	case err := <-done:
		assertNilF(t, err)
	case <-time.After(2 * time.Second):
		_ = pw.Close()
		assertTrueF(t, false, "Close hung before EOF")
	}
	_ = pw.Close()
}

func TestDownloadStreamUncompressedRoundTrip(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		contents := "test1,test2\ntest3,test4"
		stageDir, fileName := putDownloadStreamTestFile(t, sct, contents, false)
		r, err := sct.sc.DownloadStream(context.Background(), "@~/"+stageDir, fileName)
		assertNilF(t, err, "DownloadStream")
		defer func() {
			assertNilF(t, r.Close())
		}()
		got, err := io.ReadAll(r)
		assertNilF(t, err, "ReadAll")
		assertEqualF(t, string(got), contents)
	})
}

func TestDownloadStreamDecompress(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		contents := "123,test1\n456,test2\n"
		stageDir, fileName := putDownloadStreamTestFile(t, sct, contents, true)
		stage := "@~/" + stageDir

		compressed, err := sct.sc.DownloadStream(context.Background(), stage, fileName)
		assertNilF(t, err, "DownloadStream without decompress")
		defer func() {
			assertNilF(t, compressed.Close())
		}()
		gzipBytes, err := io.ReadAll(compressed)
		assertNilF(t, err)
		assertTrueF(t, len(gzipBytes) >= 2 && gzipBytes[0] == 0x1f && gzipBytes[1] == 0x8b, "expected gzip magic")

		plain, err := sct.sc.DownloadStreamWithConfig(context.Background(), stage, fileName, DownloadStreamConfig{Decompress: true})
		assertNilF(t, err, "DownloadStream with decompress")
		defer func() {
			assertNilF(t, plain.Close())
		}()
		got, err := io.ReadAll(plain)
		assertNilF(t, err)
		assertEqualF(t, string(got), contents)
	})
}

func TestDownloadStreamInvalidGzipReadError(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		stageDir, fileName := putDownloadStreamTestFile(t, sct, "not-gzip-payload", false)
		r, err := sct.sc.DownloadStreamWithConfig(context.Background(), "@~/"+stageDir, fileName, DownloadStreamConfig{Decompress: true})
		assertNilF(t, err, "open should succeed")
		assertNotNilF(t, r)
		buf := make([]byte, 8)
		n, err := r.Read(buf)
		assertEqualF(t, n, 0)
		assertNotNilF(t, err)
		var se *SnowflakeError
		assertErrorsAsF(t, err, &se)
		assertEqualF(t, se.Number, ErrDownloadStreamDecompress)
		assertNilF(t, r.Close())
		assertNilF(t, r.Close())
	})
}

func TestDownloadStreamCloseBeforeEOF(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		stageDir, fileName := putDownloadStreamTestFile(t, sct, "close-before-eof\n", false)
		r, err := sct.sc.DownloadStream(context.Background(), "@~/"+stageDir, fileName)
		assertNilF(t, err)
		done := make(chan error, 1)
		go func() {
			done <- r.Close()
		}()
		select {
		case err := <-done:
			assertNilF(t, err)
		case <-time.After(10 * time.Second):
			assertTrueF(t, false, "Close hung before EOF")
		}
	})
}

func TestDownloadStreamPrefixMatchesMultipleFiles(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		tmpDir := t.TempDir()
		stageDir := "test_download_stream_prefix_" + randomString(10)
		for _, name := range []string{"data.txt", "data.csv"} {
			fname := filepath.Join(tmpDir, name)
			assertNilF(t, os.WriteFile(fname, []byte(name+"\n"), readWriteFileMode))
			put := fmt.Sprintf("put 'file://%s' @~/%s auto_compress=false overwrite=true",
				strings.ReplaceAll(fname, "\\", "\\\\"), stageDir)
			sct.mustExec(put, []driver.Value{})
		}
		sct.Cleanup(func() {
			_, _ = sct.sc.Exec("rm @~/"+stageDir, nil)
		})

		_, err := sct.sc.DownloadStream(context.Background(), "@~/"+stageDir, "data")
		assertNotNilF(t, err, "prefix matching more than one file should fail")
		var se *SnowflakeError
		assertErrorsAsF(t, err, &se)
		assertEqualF(t, se.Number, ErrGetStreamMultipleFiles)
	})
}

func TestDownloadStreamMissingFile(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		_, err := sct.sc.DownloadStream(context.Background(), "@~", "download_stream_missing_"+randomString(8)+".txt")
		assertNotNilF(t, err, "expected missing file to fail")
		var se *SnowflakeError
		assertErrorsAsF(t, err, &se)
		assertEqualF(t, se.Number, ErrFileNotExists)
		assertEqualF(t, se.SQLState, SQLStateNoData)
	})
}

func TestDownloadStreamViaRaw(t *testing.T) {
	contents := "raw-path,ok\n"
	runDBTest(t, func(dbt *DBTest) {
		tmpDir := t.TempDir()
		fname := filepath.Join(tmpDir, "data.txt")
		assertNilF(t, os.WriteFile(fname, []byte(contents), readWriteFileMode))
		stageDir := "test_download_stream_raw_" + randomString(10)
		put := fmt.Sprintf("put 'file://%s' @~/%s auto_compress=false overwrite=true",
			strings.ReplaceAll(fname, "\\", "\\\\"), stageDir)
		dbt.mustExec(put)
		defer dbt.mustExec("rm @~/" + stageDir)

		err := dbt.conn.Raw(func(x any) error {
			sc := x.(SnowflakeConnection)
			r, err := sc.DownloadStream(context.Background(), "@~/"+stageDir, "data.txt")
			if err != nil {
				return err
			}
			defer func() {
				assertNilF(t, r.Close())
			}()
			got, err := io.ReadAll(r)
			if err != nil {
				return err
			}
			assertEqualF(t, string(got), contents)
			return nil
		})
		assertNilF(t, err, "Conn.Raw DownloadStream")
	})
}

func putDownloadStreamTestFile(t *testing.T, sct *SCTest, contents string, autoCompress bool) (stageDir, fileName string) {
	t.Helper()
	fname := filepath.Join(t.TempDir(), "data.txt")
	assertNilF(t, os.WriteFile(fname, []byte(contents), readWriteFileMode))
	stageDir = "test_download_stream_" + randomString(10)
	compress := "false"
	fileName = "data.txt"
	if autoCompress {
		compress = "true"
		fileName = "data.txt.gz"
	}
	put := fmt.Sprintf("put 'file://%s' @~/%s auto_compress=%s overwrite=true",
		strings.ReplaceAll(fname, "\\", "\\\\"), stageDir, compress)
	sct.mustExec(put, []driver.Value{})
	sct.Cleanup(func() {
		_, _ = sct.sc.Exec("rm @~/"+stageDir, nil)
	})
	return stageDir, fileName
}
