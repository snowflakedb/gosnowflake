package gosnowflake

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/snowflakedb/gosnowflake/v2/internal/query"
)

func gzipStreamBytes(t *testing.T, in []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(in); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func readAllAndClose(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("close stream: %v", err)
	}
	return got
}

func TestArrowStreamBatchToSerializable(t *testing.T) {
	t.Run("remote SSE-C", func(t *testing.T) {
		scd := &snowflakeArrowStreamChunkDownloader{
			ChunkMetas: []query.ExecResponseChunk{
				{URL: "u0"},
				{URL: "https://host/chunk1", UncompressedSize: 999},
			},
			Qrmk:              "the-qrmk",
			queryResultFormat: "arrow",
		}
		asb := &ArrowStreamBatch{idx: 1, numrows: 42, scd: scd}
		s, err := asb.ToSerializable()
		if err != nil {
			t.Fatalf("ToSerializable: %v", err)
		}
		if s.URL != "https://host/chunk1" || s.UncompressedSize != 999 {
			t.Fatalf("unexpected remote descriptor: %+v", s)
		}
		if s.Format != "arrow" || s.NumRows != 42 {
			t.Fatalf("unexpected format/rows: %+v", s)
		}
		if s.Headers[headerSseCKey] != "the-qrmk" || s.Headers[headerSseCAlgorithm] != headerSseCAes {
			t.Fatalf("SSE-C headers not set: %+v", s.Headers)
		}
	})

	t.Run("remote ChunkHeader", func(t *testing.T) {
		scd := &snowflakeArrowStreamChunkDownloader{
			ChunkMetas:        []query.ExecResponseChunk{{URL: "https://host/gcs"}},
			ChunkHeader:       map[string]string{"x-goog-meta-key": "bar"},
			queryResultFormat: "arrow",
		}
		asb := &ArrowStreamBatch{idx: 0, scd: scd}
		s, err := asb.ToSerializable()
		if err != nil {
			t.Fatalf("ToSerializable: %v", err)
		}
		if s.Headers["x-goog-meta-key"] != "bar" {
			t.Fatalf("ChunkHeader not propagated: %+v", s.Headers)
		}
		if _, ok := s.Headers[headerSseCKey]; ok {
			t.Fatalf("SSE-C header should not be present when ChunkHeader is used: %+v", s.Headers)
		}
	})

	t.Run("inline", func(t *testing.T) {
		asb := &ArrowStreamBatch{
			numrows:    5,
			inlineData: []byte("xyz"),
			scd:        &snowflakeArrowStreamChunkDownloader{queryResultFormat: "json"},
		}
		s, err := asb.ToSerializable()
		if err != nil {
			t.Fatalf("ToSerializable: %v", err)
		}
		if !bytes.Equal(s.InlineData, []byte("xyz")) || s.Format != "json" || s.NumRows != 5 || s.URL != "" {
			t.Fatalf("unexpected inline descriptor: %+v", s)
		}
	})

	t.Run("no downloader errors", func(t *testing.T) {
		asb := &ArrowStreamBatch{}
		if _, err := asb.ToSerializable(); err == nil {
			t.Fatal("expected error for a batch not backed by a downloader")
		}
	})
}

func TestArrowStreamPartitionReaderRemote(t *testing.T) {
	payload := []byte("arrow-ipc-or-json-payload-bytes")

	for _, tc := range []struct {
		name string
		body []byte
	}{
		{name: "plain", body: payload},
		{name: "gzip", body: gzipStreamBytes(t, payload)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotHeaders http.Header
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotHeaders = r.Header.Clone()
				_, _ = w.Write(tc.body)
			}))
			defer srv.Close()

			s := SerializableArrowStreamBatch{
				Format:  "arrow",
				URL:     srv.URL + "/chunk",
				Headers: map[string]string{headerSseCKey: "qrmk"},
				NumRows: 1,
			}
			reader, err := s.Reader(WithStreamHTTPClient(srv.Client()))
			if err != nil {
				t.Fatalf("Reader: %v", err)
			}
			stream, err := reader.GetStream(context.Background())
			if err != nil {
				t.Fatalf("GetStream: %v", err)
			}
			if got := readAllAndClose(t, stream); !bytes.Equal(got, payload) {
				t.Fatalf("stream = %q, want %q", got, payload)
			}
			if gotHeaders.Get(headerSseCKey) != "qrmk" {
				t.Fatalf("SSE-C header not forwarded to storage: %v", gotHeaders)
			}
		})
	}
}

func TestArrowStreamPartitionReaderInline(t *testing.T) {
	s := SerializableArrowStreamBatch{Format: "arrow", InlineData: []byte("inline-bytes"), NumRows: 1}
	reader, err := s.Reader() // no client required for inline
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	stream, err := reader.GetStream(context.Background())
	if err != nil {
		t.Fatalf("GetStream: %v", err)
	}
	if got := readAllAndClose(t, stream); !bytes.Equal(got, []byte("inline-bytes")) {
		t.Fatalf("stream = %q, want inline-bytes", got)
	}
}

func TestArrowStreamReaderRequiresClient(t *testing.T) {
	s := SerializableArrowStreamBatch{Format: "arrow", URL: "https://example.invalid/chunk", NumRows: 1}
	if _, err := s.Reader(); err == nil {
		t.Fatal("expected error when a remote batch has no HTTP client")
	}
}

func TestArrowStreamReaderExpiredURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "AccessDenied: Request has expired", http.StatusForbidden)
	}))
	defer srv.Close()

	s := SerializableArrowStreamBatch{Format: "arrow", URL: srv.URL + "/chunk", NumRows: 1}
	reader, err := s.Reader(WithStreamHTTPClient(srv.Client()))
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	if _, err := reader.GetStream(context.Background()); err == nil {
		t.Fatal("expected an error fetching an expired (403) URL")
	}
}

func TestSerializableArrowStreamBatchJSONRoundTrip(t *testing.T) {
	orig := SerializableArrowStreamBatch{
		Format:           "arrow",
		InlineData:       []byte{1, 2, 3, 4},
		URL:              "https://example.invalid/chunk",
		Headers:          map[string]string{headerSseCKey: "k"},
		NumRows:          7,
		UncompressedSize: 4096,
	}
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got SerializableArrowStreamBatch
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Format != orig.Format || !bytes.Equal(got.InlineData, orig.InlineData) ||
		got.URL != orig.URL || got.NumRows != orig.NumRows || got.UncompressedSize != orig.UncompressedSize ||
		got.Headers[headerSseCKey] != "k" {
		t.Fatalf("descriptor did not round-trip: %+v", got)
	}
}

func TestQueryArrowStreamSerializableRoundTrip(t *testing.T) {
	runSnowflakeConnTest(t, func(sct *SCTest) {
		numrows := 50000
		query := fmt.Sprintf(selectRandomGenerator, numrows)

		loader, err := sct.sc.QueryArrowStream(sct.sc.ctx, query)
		assertNilF(t, err)
		if p, ok := loader.(QueryResultFormatProvider); ok {
			assertEqualE(t, p.QueryResultFormat(), "arrow")
		}

		batches, err := loader.GetBatches()
		assertNilF(t, err)
		assertTrueF(t, len(batches) > 0, "should have at least one batch")

		ctx := context.Background()
		workerClient := &http.Client{} // a worker's client: no Snowflake connection
		totalRows := 0
		sawRemoteChunk := false

		for i := range batches {
			// Capture the descriptor before reading the original stream.
			desc, err := batches[i].ToSerializable()
			assertNilF(t, err)

			// Driver path: read the original chunk bytes.
			origStream, err := batches[i].GetStream(ctx)
			assertNilF(t, err)
			origBytes, err := io.ReadAll(origStream)
			assertNilF(t, err)
			assertNilF(t, origStream.Close())

			// Ship the descriptor as JSON and decode it on the "worker".
			data, err := json.Marshal(desc)
			assertNilF(t, err)
			var decoded SerializableArrowStreamBatch
			assertNilF(t, json.Unmarshal(data, &decoded))
			if decoded.URL != "" {
				sawRemoteChunk = true
			}

			// Distributed path: reconstruct and read with the bare client.
			reader, err := decoded.Reader(WithStreamHTTPClient(workerClient))
			assertNilF(t, err)
			reconStream, err := reader.GetStream(ctx)
			assertNilF(t, err)
			reconBytes, err := io.ReadAll(reconStream)
			assertNilF(t, err)
			assertNilF(t, reconStream.Close())

			// The distributed path must reproduce the driver path exactly.
			assertTrueF(t, bytes.Equal(origBytes, reconBytes),
				fmt.Sprintf("batch %d: reconstructed bytes differ from original", i))

			// Decode the reconstructed Arrow IPC end-to-end and count rows.
			if len(reconBytes) > 0 {
				rr, err := ipc.NewReader(bytes.NewReader(reconBytes))
				assertNilF(t, err)
				for rr.Next() {
					totalRows += int(rr.Record().NumRows())
				}
				assertNilF(t, rr.Err())
				rr.Release()
			}
		}

		assertTrueF(t, sawRemoteChunk, "expected at least one remote (downloaded) chunk; increase numrows")
		assertEqualE(t, totalRows, numrows)
	})
}
