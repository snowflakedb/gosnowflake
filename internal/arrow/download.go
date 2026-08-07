package arrow

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// errorBodyLimit bounds how much of a non-200 response body is read for error messages.
const errorBodyLimit = 4096

// chunkStream adapts an HTTP response body (optionally gzip-decompressed) into an
// io.ReadCloser whose Close releases both the gzip reader and the underlying body.
type chunkStream struct {
	r    io.Reader
	gz   *gzip.Reader
	body io.ReadCloser
}

func (c *chunkStream) Read(p []byte) (int, error) { return c.r.Read(p) }

func (c *chunkStream) Close() error {
	var err error
	if c.gz != nil {
		err = c.gz.Close()
	}
	if bodyErr := c.body.Close(); bodyErr != nil && err == nil {
		err = bodyErr
	}
	return err
}

// OpenChunkStream performs an HTTP GET against a presigned result-chunk URL and returns
// a reader over the (gzip-decompressed if needed) payload. The provided client is used
// as-is: request cancellation and timeouts come from ctx and the client's own settings.
//
// No Snowflake session is required — the URL is presigned and any decryption is performed
// server-side by cloud storage using the SSE-C key passed in headers. The caller must
// Close the returned stream.
func OpenChunkStream(ctx context.Context, client *http.Client, url string, headers map[string]string) (io.ReadCloser, error) {
	if client == nil {
		return nil, fmt.Errorf("arrow: no http client provided for chunk download")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("arrow: creating chunk request: %w", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("arrow: fetching chunk: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, errorBodyLimit))
		return nil, fmt.Errorf("arrow: failed to get chunk: HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(b))
	}

	bufStream := bufio.NewReader(resp.Body)
	// Peek is best-effort: a short or empty payload simply skips gzip detection and lets
	// the IPC reader surface any real decode error.
	magic, _ := bufStream.Peek(2)
	cs := &chunkStream{body: resp.Body}
	if len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		gz, err := gzip.NewReader(bufStream)
		if err != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("arrow: creating gzip reader: %w", err)
		}
		cs.gz = gz
		cs.r = gz
	} else {
		cs.r = bufStream
	}
	return cs, nil
}

// DecodeIPCRecords reads all Arrow record batches from an IPC stream. Returned records are
// retained and must be released by the caller. On error, any records already read are released.
//
// Options are passed through to ipc.NewReader; in particular ipc.WithAllocator controls the
// memory allocator. When none is provided the Arrow default allocator is used.
func DecodeIPCRecords(r io.Reader, opts ...ipc.Option) (*[]arrow.Record, int, error) {
	reader, err := ipc.NewReader(r, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("arrow: creating ipc reader: %w", err)
	}
	defer reader.Release()

	var records []arrow.Record
	rowCount := 0
	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		records = append(records, rec)
		rowCount += int(rec.NumRows())
	}
	if err := reader.Err(); err != nil {
		for _, rec := range records {
			rec.Release()
		}
		return nil, 0, fmt.Errorf("arrow: reading ipc records: %w", err)
	}
	return &records, rowCount, nil
}

// DownloadBatchRecords resolves a single batch, described by desc, to its raw (untransformed)
// Arrow records. If desc.InlineData is set it is decoded directly; otherwise the chunk is
// downloaded from desc.URL using the injected client. Options are passed through to the IPC
// reader (e.g. ipc.WithAllocator). The returned records are retained and must be released by
// the caller (ArrowBatch.Fetch does this after transforming them).
func DownloadBatchRecords(ctx context.Context, client *http.Client, desc ChunkDescriptor, opts ...ipc.Option) (*[]arrow.Record, int, error) {
	if desc.InlineDataBase64 != "" {
		inline, err := base64.StdEncoding.DecodeString(desc.InlineDataBase64)
		if err != nil {
			return nil, 0, fmt.Errorf("arrow: decoding inline batch base64: %w", err)
		}
		return DecodeIPCRecords(bytes.NewReader(inline), opts...)
	}
	if desc.URL == "" {
		empty := make([]arrow.Record, 0)
		return &empty, 0, nil
	}
	stream, err := OpenChunkStream(ctx, client, desc.URL, desc.Headers)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = stream.Close() }()
	return DecodeIPCRecords(stream, opts...)
}
