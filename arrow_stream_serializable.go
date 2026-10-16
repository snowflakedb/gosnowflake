package gosnowflake

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
)

// SerializableArrowStreamBatch is a portable, self-contained description of an
// ArrowStreamBatch (see QueryArrowStream / ArrowStreamLoader). It can be marshaled
// (e.g. with encoding/json), shipped to another process or machine that has no Snowflake
// connection, and turned back into a readable stream with Reader. This enables distributed
// fetch: partition a result set on one node and download the chunks in parallel across many
// (e.g. from an ADBC driver's ExecutePartitions / ReadPartition).
//
// The descriptor carries only the chunk address and payload format; it deliberately does not
// carry column metadata or timezone. A consumer that needs to decode the bytes obtains
// RowTypes() and Location() from the ArrowStreamLoader and carries them alongside.
//
// Security: for a remote batch, Headers carries the SSE-C decryption key and URL is a
// presigned bearer capability granting read access to the chunk. Treat a serialized batch as
// sensitive and transmit it only over secure channels.
//
// Expiry: presigned URLs are valid only for a server-controlled window (typically a few
// hours). A remote batch must be shipped and fetched within it. Inline batches (InlineData
// set) embed their bytes and never expire.
type SerializableArrowStreamBatch struct {
	// Format is the payload format of the stream, "arrow" or "json".
	Format string `json:"format"`
	// InlineData holds the batch bytes embedded in the query response (the first batch).
	// When set, the batch is local and no download is performed.
	InlineData []byte `json:"inlineData,omitempty"`
	// URL is the presigned cloud-storage URL for a remote chunk (empty for inline batches).
	URL string `json:"url,omitempty"`
	// Headers are the HTTP headers required to fetch URL, including the SSE-C key.
	Headers map[string]string `json:"headers,omitempty"`
	// NumRows is the number of rows the metadata reported for this batch.
	NumRows int64 `json:"numRows"`
	// UncompressedSize is the reported uncompressed size of the chunk in bytes.
	UncompressedSize int64 `json:"uncompressedSize,omitempty"`
}

// ToSerializable converts a stream batch into a portable descriptor for distributed fetch.
// Call it before GetStream. The batch must have been produced by ArrowStreamLoader.GetBatches
// (i.e. be backed by a downloader).
func (asb *ArrowStreamBatch) ToSerializable() (SerializableArrowStreamBatch, error) {
	if asb.scd == nil {
		return SerializableArrowStreamBatch{}, fmt.Errorf(
			"arrow stream batch is not backed by a downloader and cannot be serialized")
	}
	s := SerializableArrowStreamBatch{
		Format:  asb.scd.queryResultFormat,
		NumRows: asb.numrows,
	}
	if asb.inlineData != nil {
		s.InlineData = asb.inlineData
		return s, nil
	}
	meta := asb.scd.ChunkMetas[asb.idx]
	s.URL = meta.URL
	s.UncompressedSize = meta.UncompressedSize
	s.Headers = streamChunkHeaders(asb.scd.ChunkHeader, asb.scd.Qrmk)
	return s, nil
}

// streamChunkHeaders builds the HTTP headers used to fetch a result chunk on the stream path.
// It mirrors downloadChunkStreamHelper: use the server-provided ChunkHeader map when present
// (GCS/Azure), otherwise the S3 SSE-C headers carrying the query result master key (qrmk).
func streamChunkHeaders(chunkHeader map[string]string, qrmk string) map[string]string {
	headers := make(map[string]string)
	if len(chunkHeader) > 0 {
		maps.Copy(headers, chunkHeader)
	} else {
		headers[headerSseCAlgorithm] = headerSseCAes
		headers[headerSseCKey] = qrmk
	}
	return headers
}

// streamOpts holds options for reconstructing a stream reader on a worker node.
type streamOpts struct {
	client *http.Client
}

// StreamOption configures SerializableArrowStreamBatch.Reader.
type StreamOption func(*streamOpts)

// WithStreamHTTPClient injects the HTTP client used to download a remote chunk. It is
// required for a batch that references a URL; the worker has no Snowflake connection to build
// one from. This is also the seam for configuring proxy/TLS/timeouts and transport retries.
func WithStreamHTTPClient(client *http.Client) StreamOption {
	return func(o *streamOpts) { o.client = client }
}

// ArrowStreamPartitionReader reads a single serialized stream batch on a worker node. Its
// GetStream mirrors ArrowStreamBatch.GetStream so it can be consumed the same way (fed into
// ipc.NewReader, or used through the ADBC driver's stream reader).
type ArrowStreamPartitionReader struct {
	inlineData []byte
	url        string
	headers    map[string]string
	client     *http.Client
}

// Reader reconstructs a readable stream from a serialized batch. A remote batch (URL set)
// requires an *http.Client via WithStreamHTTPClient; an inline batch needs none.
func (s SerializableArrowStreamBatch) Reader(opts ...StreamOption) (*ArrowStreamPartitionReader, error) {
	var o streamOpts
	for _, opt := range opts {
		opt(&o)
	}
	remote := len(s.InlineData) == 0 && s.URL != ""
	if remote && o.client == nil {
		return nil, fmt.Errorf("gosnowflake: a remote arrow stream batch requires an *http.Client (use WithStreamHTTPClient)")
	}
	return &ArrowStreamPartitionReader{
		inlineData: s.InlineData,
		url:        s.URL,
		headers:    s.Headers,
		client:     o.client,
	}, nil
}

// GetStream returns a stream of bytes for the batch, downloading the chunk first if it is
// remote. The content is Arrow IPC or JSON depending on the batch's Format. Close the
// returned stream when done.
func (r *ArrowStreamPartitionReader) GetStream(ctx context.Context) (io.ReadCloser, error) {
	if len(r.inlineData) > 0 {
		return newCancelableStream(ctx, io.NopCloser(bytes.NewReader(r.inlineData))), nil
	}
	rr, err := openStandaloneChunkStream(ctx, r.client, r.url, r.headers)
	if err != nil {
		return nil, err
	}
	return newCancelableStream(ctx, rr), nil
}

// openStandaloneChunkStream performs an HTTP GET against a presigned result-chunk URL using
// the injected client (no Snowflake connection) and returns a reader over the (gzip-inflated
// if needed) payload. It mirrors the transport handling in downloadChunkStreamHelper. The
// caller must Close the returned stream.
func openStandaloneChunkStream(ctx context.Context, client *http.Client, url string, headers map[string]string) (io.ReadCloser, error) {
	if client == nil {
		return nil, fmt.Errorf("gosnowflake: no http client provided for chunk download")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, &SnowflakeError{
			Number:   ErrFailedToGetChunk,
			SQLState: SQLStateConnectionFailure,
			Message:  fmt.Sprintf("failed to get chunk. HTTP: %d, body: %s", resp.StatusCode, bytes.TrimSpace(b)),
		}
	}

	bufStream := bufio.NewReader(resp.Body)
	// Peek is best-effort: a short or empty payload simply skips gzip detection.
	gzipMagic, _ := bufStream.Peek(2)
	if len(gzipMagic) == 2 && gzipMagic[0] == 0x1f && gzipMagic[1] == 0x8b {
		gz, err := gzip.NewReader(bufStream)
		if err != nil {
			_ = resp.Body.Close()
			return nil, err
		}
		return &streamWrapReader{Reader: gz, wrapped: resp.Body}, nil
	}
	return &streamWrapReader{Reader: bufStream, wrapped: resp.Body}, nil
}
