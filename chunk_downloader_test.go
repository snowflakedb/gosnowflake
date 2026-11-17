package gosnowflake

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	ia "github.com/snowflakedb/gosnowflake/v2/internal/arrow"
	"github.com/snowflakedb/gosnowflake/v2/internal/query"
)

func newCancelableTestChunkDownloader(ctx context.Context, body io.ReadCloser) *snowflakeChunkDownloader {
	one := "1"
	return &snowflakeChunkDownloader{
		sc: &snowflakeConn{
			cfg:        &Config{},
			rest:       &snowflakeRestful{RequestTimeout: time.Second},
			syncParams: syncParams{params: map[string]*string{clientPrefetchThreadsKey: &one}},
		},
		ctx:                ctx,
		CellCount:          1,
		ChunkMetas:         []query.ExecResponseChunk{{URL: "https://example.com/chunk", RowCount: 1}},
		TotalRowIndex:      int64(-1),
		FuncDownload:       downloadChunk,
		FuncDownloadHelper: downloadChunkHelper,
		FuncGet: func(context.Context, *snowflakeConn, string, map[string]string, time.Duration) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
		},
		QueryResultFormat: "json",
		RowSet: rowSetType{
			RowType: []query.ExecResponseRowType{{Type: "TEXT"}},
		},
	}
}

func buildArrowChunkBytes(t *testing.T) []byte {
	t.Helper()

	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	bldr := array.NewRecordBuilder(pool, schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	rec := bldr.NewRecord()
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(pool))
	assertNilF(t, w.Write(rec), "writing arrow IPC stream should succeed")
	assertNilF(t, w.Close(), "closing arrow IPC writer should succeed")
	return append([]byte(nil), buf.Bytes()...)
}

func TestChunkDownloaderDoesNotStartWhenArrowParsingCausesError(t *testing.T) {
	tcs := []string{
		"invalid base64",
		"aW52YWxpZCBhcnJvdw==", // valid base64, but invalid arrow
	}
	for _, tc := range tcs {
		t.Run(tc, func(t *testing.T) {
			scd := snowflakeChunkDownloader{
				ctx:               context.Background(),
				QueryResultFormat: "arrow",
				RowSet: rowSetType{
					RowSetBase64: tc,
				},
			}

			err := scd.start()

			assertNotNilF(t, err)
		})
	}
}

func TestDownloadChunkHelperCancellationUnblocksStalledRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	terminalErr := errors.New("read from closed body")
	body := newBlockingReadCloser([]byte(`["value"]`), terminalErr)
	scd := newCancelableTestChunkDownloader(ctx, body)

	errCh := make(chan error, 1)
	go func() {
		errCh <- downloadChunkHelper(ctx, scd, 0)
	}()

	readBlocked := false
	select {
	case <-body.blockingReadStarted:
		readBlocked = true
	case <-time.After(100 * time.Millisecond):
	}
	assertTrueF(t, readBlocked, "decode should block on the live response body before cancellation")

	cancel()

	var err error
	readFinished := false
	select {
	case err = <-errCh:
		readFinished = true
	case <-time.After(200 * time.Millisecond):
	}
	assertTrueF(t, readFinished, "cancellation should unblock the chunk download promptly")
	assertErrIsF(t, err, context.Canceled, "interrupted chunk downloads should surface context cancellation")
	assertFalseF(t, errors.Is(err, terminalErr), "interrupted chunk downloads should not leak transport close errors")
	assertTrueF(t, body.closed.Load(), "cancellation should close the underlying response body")
}

func TestDownloadChunkHelperArrowCancellationUnblocksStalledRead(t *testing.T) {
	ctx, cancel := context.WithCancel(ia.EnableArrowBatches(context.Background()))
	defer cancel()

	terminalErr := errors.New("read from closed body")
	body := newBlockingReadCloser(buildArrowChunkBytes(t), terminalErr)
	body.firstChunkSize = len(body.payload) / 2
	scd := newCancelableTestChunkDownloader(ctx, body)
	scd.QueryResultFormat = "arrow"
	scd.pool = memory.DefaultAllocator
	scd.rawBatches = []*rawArrowBatchData{{}}

	errCh := make(chan error, 1)
	go func() {
		errCh <- downloadChunkHelper(ctx, scd, 0)
	}()

	readBlocked := false
	select {
	case <-body.blockingReadStarted:
		readBlocked = true
	case <-time.After(100 * time.Millisecond):
	}
	assertTrueF(t, readBlocked, "arrow decode should block on the live response body before cancellation")

	cancel()

	var err error
	readFinished := false
	select {
	case err = <-errCh:
		readFinished = true
	case <-time.After(200 * time.Millisecond):
	}
	assertTrueF(t, readFinished, "cancellation should unblock the stalled Arrow chunk download promptly")
	assertErrIsF(t, err, context.Canceled, "interrupted Arrow chunk downloads should surface context cancellation")
	assertFalseF(t, errors.Is(err, terminalErr), "interrupted Arrow chunk downloads should not leak transport close errors")
	assertTrueF(t, body.closed.Load(), "cancellation should close the underlying response body")
}

// recordingRoundTripper records what net/http did with a chunk request:
// the Accept-Encoding the driver set, and whether net/http decompressed the body.
type recordingRoundTripper struct {
	inner          http.RoundTripper
	acceptEncoding string
	uncompressed   bool
}

func (rt *recordingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := rt.inner.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	rt.acceptEncoding = req.Header.Get("Accept-Encoding")
	rt.uncompressed = resp.Uncompressed
	return resp, nil
}

func TestDownloadChunkHelperRequestsGzipAndDecodesChunk(t *testing.T) {
	payload := []byte(`["value"]`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(gzipBytes(payload))
	}))
	defer server.Close()

	rt := &recordingRoundTripper{inner: http.DefaultTransport}
	scd := newCancelableTestChunkDownloader(context.Background(), nil)
	scd.ChunkMetas[0].URL = server.URL + "/chunk"
	scd.FuncGet = getChunk
	scd.sc.rest.Client = &http.Client{Transport: rt}
	scd.sc.rest.MaxRetryCount = 1
	scd.sc.currentTimeProvider = constTimeProvider(0)
	scd.Chunks = make(map[int][]chunkRowType)
	scd.ChunksMutex = &sync.Mutex{}

	err := downloadChunkHelper(context.Background(), scd, 0)
	assertNilF(t, err, "chunk download should succeed")
	assertEqualE(t, rt.acceptEncoding, "gzip", "chunk request should ask for gzip explicitly")
	assertFalseE(t, rt.uncompressed, "net/http should not decompress the chunk, so the driver's gzip check should decode it")
	assertEqualF(t, len(scd.Chunks[0]), 1, "chunk should contain one row")
	assertEqualF(t, len(scd.Chunks[0][0].RowSet), 1, "row should contain one value")
	assertNotNilF(t, scd.Chunks[0][0].RowSet[0], "value should not be NULL")
	assertEqualE(t, *scd.Chunks[0][0].RowSet[0], "value", "decoded chunk value should match the payload")
}

func TestEnsureGzipAcceptEncoding(t *testing.T) {
	tcs := []struct {
		name     string
		headers  map[string]string
		expected map[string]string
	}{
		{
			name:     "no Accept-Encoding sets gzip",
			headers:  map[string]string{},
			expected: map[string]string{"Accept-Encoding": "gzip"},
		},
		{
			name:     "server Accept-Encoding is kept",
			headers:  map[string]string{"Accept-Encoding": "identity"},
			expected: map[string]string{"Accept-Encoding": "identity"},
		},
		{
			name:     "server lowercase accept-encoding is kept without a duplicate key",
			headers:  map[string]string{"accept-encoding": "identity"},
			expected: map[string]string{"accept-encoding": "identity"},
		},
		{
			name:     "other headers are left alone",
			headers:  map[string]string{"x-amz-server-side-encryption": "AES256"},
			expected: map[string]string{"x-amz-server-side-encryption": "AES256", "Accept-Encoding": "gzip"},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ensureGzipAcceptEncoding(tc.headers)
			assertDeepEqualE(t, tc.headers, tc.expected, "headers should match after ensuring gzip")
		})
	}
}

func TestChunkDownloaderNextCancellationUnblocksWaitingRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	body := newBlockingReadCloser([]byte(`["value"]`), errors.New("read from closed body"))
	scd := newCancelableTestChunkDownloader(ctx, body)

	assertNilF(t, scd.start(), "chunk downloader should start")

	nextErrCh := make(chan error, 1)
	go func() {
		_, err := scd.next()
		nextErrCh <- err
	}()

	readBlocked := false
	select {
	case <-body.blockingReadStarted:
		readBlocked = true
	case <-time.After(100 * time.Millisecond):
	}
	assertTrueF(t, readBlocked, "background chunk decode should block on the live response body")

	cancel()

	var err error
	nextFinished := false
	select {
	case err = <-nextErrCh:
		nextFinished = true
	case <-time.After(200 * time.Millisecond):
	}
	assertTrueF(t, nextFinished, "cancellation should unblock waiting row reads promptly")
	assertErrIsF(t, err, context.Canceled, "waiting row reads should surface context cancellation")
	assertTrueF(t, body.closed.Load(), "cancellation should close the underlying response body")
}

func TestWithArrowBatchesWhenQueryReturnsNoRowsWhenUsingNativeGoSQLInterface(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		var rows driver.Rows
		var err error
		err = dbt.conn.Raw(func(x any) error {
			rows, err = x.(driver.QueryerContext).QueryContext(ia.EnableArrowBatches(context.Background()), "SELECT 1 WHERE 0 = 1", nil)
			return err
		})
		assertNilF(t, err)
		rows.Close()
	})
}

func TestWithArrowBatchesWhenQueryReturnsRowsAndReadingRows(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		rows := dbt.mustQueryContext(ia.EnableArrowBatches(context.Background()), "SELECT 1")
		defer rows.Close()
		assertFalseF(t, rows.Next())
	})
}

func TestWithArrowBatchesWhenQueryReturnsNoRowsAndReadingRows(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		rows := dbt.mustQueryContext(ia.EnableArrowBatches(context.Background()), "SELECT 1 WHERE 1 = 0")
		defer rows.Close()
		assertFalseF(t, rows.Next())
	})
}

func TestWithArrowBatchesWhenQueryReturnsNoRowsAndReadingArrowBatchData(t *testing.T) {
	runDBTest(t, func(dbt *DBTest) {
		var rows driver.Rows
		var err error
		err = dbt.conn.Raw(func(x any) error {
			rows, err = x.(driver.QueryerContext).QueryContext(ia.EnableArrowBatches(context.Background()), "SELECT 1 WHERE 1 = 0", nil)
			return err
		})
		assertNilF(t, err)
		defer rows.Close()
		provider := rows.(SnowflakeRows).(ia.BatchDataProvider)
		info, err := provider.GetArrowBatches()
		assertNilF(t, err)
		assertEmptyE(t, info.Batches)
	})
}
