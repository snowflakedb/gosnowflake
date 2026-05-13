package gosnowflake

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snowflakedb/gosnowflake/v2/internal/query"
)

type finalEOFStreamBody struct {
	blockingReadStarted chan struct{}
	unblockRead         chan struct{}

	blockOnce sync.Once
	closeOnce sync.Once
	closed    atomic.Bool
	delivered bool
	payload   []byte
}

func newFinalEOFStreamBody(payload []byte) *finalEOFStreamBody {
	return &finalEOFStreamBody{
		blockingReadStarted: make(chan struct{}),
		unblockRead:         make(chan struct{}),
		payload:             append([]byte(nil), payload...),
	}
}

func (b *finalEOFStreamBody) Read(p []byte) (int, error) {
	if b.delivered {
		return 0, io.EOF
	}
	b.blockOnce.Do(func() {
		close(b.blockingReadStarted)
	})
	<-b.unblockRead
	b.delivered = true
	copy(p, b.payload)
	return len(b.payload), io.EOF
}

func (b *finalEOFStreamBody) Close() error {
	b.closed.Store(true)
	b.closeOnce.Do(func() {
		close(b.unblockRead)
	})
	return nil
}

type closeCountingReadCloser struct {
	closeCalls  atomic.Int32
	secondClose chan struct{}
}

func (c *closeCountingReadCloser) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func (c *closeCountingReadCloser) Close() error {
	if c.closeCalls.Add(1) == 2 && c.secondClose != nil {
		close(c.secondClose)
	}
	return nil
}

func newTestArrowStreamBatch(body io.ReadCloser) ArrowStreamBatch {
	return ArrowStreamBatch{
		idx: 0,
		scd: &snowflakeArrowStreamChunkDownloader{
			sc: &snowflakeConn{
				rest: &snowflakeRestful{RequestTimeout: time.Second},
			},
			ChunkMetas: []query.ExecResponseChunk{{
				URL:      "https://example.com/chunk",
				RowCount: 1,
			}},
			FuncGet: func(context.Context, *snowflakeConn, string, map[string]string, time.Duration) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       body,
				}, nil
			},
		},
	}
}

func gzipBody(payload []byte) io.ReadCloser {
	return io.NopCloser(bytes.NewReader(gzipBytes(payload)))
}

func gzipBytes(payload []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(payload)
	_ = zw.Close()
	return buf.Bytes()
}

func TestArrowStreamBatchGetStreamCancellationUnblocksStalledRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	body := newBlockingReadCloser([]byte("ok"), io.EOF)
	batch := newTestArrowStreamBatch(body)

	stream, err := batch.GetStream(ctx)
	assertNilF(t, err, "GetStream should succeed")
	defer func() {
		_ = stream.Close()
	}()

	type readResult struct {
		data []byte
		err  error
	}

	readResultCh := make(chan readResult, 1)
	go func() {
		data, readErr := io.ReadAll(stream)
		readResultCh <- readResult{data: data, err: readErr}
	}()

	readBlocked := false
	select {
	case <-body.blockingReadStarted:
		readBlocked = true
	case <-time.After(100 * time.Millisecond):
	}
	assertTrueF(t, readBlocked, "read should block after the stream opens")

	cancel()

	var result readResult
	readFinished := false
	select {
	case result = <-readResultCh:
		readFinished = true
	case <-time.After(200 * time.Millisecond):
	}
	assertTrueF(t, readFinished, "cancellation should unblock a stalled read promptly")
	assertEqualF(t, string(result.data), "ok", "buffered bytes should still be returned before cancellation")
	assertErrIsF(t, result.err, context.Canceled, "stalled read should surface cancellation")
	assertTrueF(t, body.closed.Load(), "cancellation should close the underlying body")
}

func TestArrowStreamBatchGetStreamCancellationNormalizesCloseErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	terminalErr := errors.New("read from closed body")
	body := newBlockingReadCloser([]byte("ok"), terminalErr)
	batch := newTestArrowStreamBatch(body)

	stream, err := batch.GetStream(ctx)
	assertNilF(t, err, "GetStream should succeed")
	defer func() {
		_ = stream.Close()
	}()

	type readResult struct {
		data []byte
		err  error
	}

	readResultCh := make(chan readResult, 1)
	go func() {
		data, readErr := io.ReadAll(stream)
		readResultCh <- readResult{data: data, err: readErr}
	}()

	readBlocked := false
	select {
	case <-body.blockingReadStarted:
		readBlocked = true
	case <-time.After(100 * time.Millisecond):
	}
	assertTrueF(t, readBlocked, "read should block after the stream opens")

	cancel()

	result := <-readResultCh
	assertEqualF(t, string(result.data), "ok", "buffered bytes should still be returned before cancellation")
	assertErrIsF(t, result.err, context.Canceled, "canceled reads should surface context cancellation")
	assertFalseF(t, errors.Is(result.err, terminalErr), "canceled reads should not leak the transport close error")
}

func TestArrowStreamBatchGetStreamCancellationNormalizesGzipBlockedRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	terminalErr := errors.New("read from closed body")
	body := newBlockingReadCloser(gzipBytes([]byte("ok")), terminalErr)
	body.firstChunkSize = len(body.payload) / 2
	batch := newTestArrowStreamBatch(body)

	stream, err := batch.GetStream(ctx)
	assertNilF(t, err, "GetStream should succeed")
	defer func() {
		_ = stream.Close()
	}()

	type readResult struct {
		data []byte
		err  error
	}

	readResultCh := make(chan readResult, 1)
	go func() {
		data, readErr := io.ReadAll(stream)
		readResultCh <- readResult{data: data, err: readErr}
	}()

	readBlocked := false
	select {
	case <-body.blockingReadStarted:
		readBlocked = true
	case <-time.After(100 * time.Millisecond):
	}
	assertTrueF(t, readBlocked, "gzip read should block waiting for more compressed bytes")

	cancel()

	result := <-readResultCh
	assertEqualF(t, string(result.data), "ok", "buffered gzip payload should still be returned before cancellation")
	assertErrIsF(t, result.err, context.Canceled, "canceled gzip reads should surface context cancellation")
	assertFalseF(t, errors.Is(result.err, terminalErr), "canceled gzip reads should not leak the transport close error")
	assertTrueF(t, body.closed.Load(), "cancellation should close the underlying body")
}

func TestArrowStreamBatchGetStreamAlreadyCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch := ArrowStreamBatch{
		idx: 0,
		scd: &snowflakeArrowStreamChunkDownloader{
			sc: &snowflakeConn{
				rest: &snowflakeRestful{RequestTimeout: time.Second},
			},
			ChunkMetas: []query.ExecResponseChunk{{
				URL:      "https://example.com/chunk",
				RowCount: 1,
			}},
			FuncGet: func(ctx context.Context, _ *snowflakeConn, _ string, _ map[string]string, _ time.Duration) (*http.Response, error) {
				return nil, ctx.Err()
			},
		},
	}

	stream, err := batch.GetStream(ctx)
	assertNilF(t, stream, "GetStream should not return a stream for an already-canceled context")
	assertErrIsF(t, err, context.Canceled, "GetStream should surface the canceled context before opening the stream")
}

func TestCancelableStreamCancellationAfterFinalPayloadPreservesCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	body := newFinalEOFStreamBody([]byte("ok"))
	stream := newCancelableStream(ctx, body)
	defer func() {
		assertNilF(t, stream.Close(), "closing stream should succeed")
	}()

	type readResult struct {
		data []byte
		err  error
	}

	readResultCh := make(chan readResult, 1)
	go func() {
		data, readErr := io.ReadAll(stream)
		readResultCh <- readResult{data: data, err: readErr}
	}()

	readBlocked := false
	select {
	case <-body.blockingReadStarted:
		readBlocked = true
	case <-time.After(100 * time.Millisecond):
	}
	assertTrueF(t, readBlocked, "read should block before the final payload is delivered")

	cancel()

	result := <-readResultCh
	assertEqualF(t, string(result.data), "ok", "final payload should still be returned")
	assertNilF(t, result.err, "final payload delivery should still complete successfully")
}

func TestCancelableStreamCloseStopsWatcherBeforeCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	body := &closeCountingReadCloser{secondClose: make(chan struct{})}
	stream := newCancelableStream(ctx, body)

	assertNilF(t, stream.Close(), "closing stream should succeed")
	assertEqualF(t, body.closeCalls.Load(), int32(1), "closing the stream should close the body exactly once")

	cancel()

	secondClose := false
	select {
	case <-body.secondClose:
		secondClose = true
	case <-time.After(50 * time.Millisecond):
	}

	assertFalseF(t, secondClose, "canceling after close should not re-close the body")
	assertEqualF(t, body.closeCalls.Load(), int32(1), "canceling after close should not re-close the body")
}

func TestArrowStreamBatchGetStreamPreservesCompletedEOF(t *testing.T) {
	payload := []byte(`{"key":"value"}`)

	testCases := []struct {
		name string
		body io.ReadCloser
	}{
		{
			name: "plain",
			body: io.NopCloser(bytes.NewReader(payload)),
		},
		{
			name: "gzip",
			body: gzipBody(payload),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			batch := newTestArrowStreamBatch(tc.body)
			stream, err := batch.GetStream(context.Background())
			assertNilF(t, err, "GetStream should succeed")
			defer func() {
				assertNilF(t, stream.Close(), "closing stream should succeed")
			}()

			data, err := io.ReadAll(stream)
			assertNilF(t, err, "completed streams should read successfully")
			assertEqualF(t, string(data), string(payload), "stream content should match the response body")

			n, err := stream.Read(make([]byte, 1))
			assertEqualF(t, n, 0, "completed streams should not return extra bytes")
			assertErrIsF(t, err, io.EOF, "completed streams should preserve EOF")
		})
	}
}
