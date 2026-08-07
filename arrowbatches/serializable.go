package arrowbatches

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	ia "github.com/snowflakedb/gosnowflake/v2/internal/arrow"
	"github.com/snowflakedb/gosnowflake/v2/internal/query"
)

// arrowFormatName is the only result format arrow batches carry today. The Format
// field is kept for forward compatibility with the streaming/JSON result path.
const arrowFormatName = "arrow"

// SerializableArrowBatch is a portable, self-contained description of an ArrowBatch.
// It can be marshaled (e.g. with encoding/json), shipped to another machine that has
// no Snowflake connection, and turned back into an ArrowBatch with ToArrowBatch. This
// enables distributed fetch: partition a result set on one node and download the
// chunks in parallel across many.
//
// Security: for remote batches, Headers carries the SSE-C decryption key and URL is a
// presigned bearer capability granting read access to the chunk. Treat a serialized
// batch as sensitive and transmit it only over secure channels.
//
// Expiry: presigned URLs are valid only for a server-controlled window (typically a few
// hours). A remote SerializableArrowBatch must be shipped and fetched within it. Inline
// batches (InlineData set) embed their data and never expire.
type SerializableArrowBatch struct {
	// Format is the chunk payload format; currently always "arrow".
	Format string `json:"format"`
	// InlineData holds the base64-encoded Arrow IPC bytes for the first batch (carried
	// verbatim from the query response). When set, the batch is local and no download is
	// performed; the bytes are base64-decoded only when the batch is materialized.
	InlineData string `json:"inlineData,omitempty"`
	// URL is the presigned cloud-storage URL for a remote chunk (empty for inline batches).
	URL string `json:"url,omitempty"`
	// Headers are the HTTP headers required to fetch URL, including the SSE-C key.
	Headers map[string]string `json:"headers,omitempty"`
	// RowCount is the number of rows in the batch.
	RowCount int `json:"rowCount"`
	// UncompressedSize is the reported uncompressed size of the chunk in bytes.
	UncompressedSize int64 `json:"uncompressedSize,omitempty"`
	// RowTypes is the column metadata needed to transform the raw records.
	RowTypes []query.ExecResponseRowType `json:"rowTypes"`
	// TimezoneName is the batch's timezone name (loc.String()).
	TimezoneName string `json:"timezoneName,omitempty"`
	// TimezoneOffsetSeconds is the batch's timezone offset, used as a fallback when the
	// worker cannot resolve TimezoneName (e.g. fixed-offset zones, or missing tzdata).
	TimezoneOffsetSeconds int `json:"timezoneOffsetSeconds,omitempty"`
	// ConversionOptions carries the options controlling how the raw Arrow data is converted
	// (timestamp handling, higher precision, UTF-8 validation). It is embedded so its fields
	// appear at the top level of the serialized form, and it is the single place new
	// conversion options flow through — see ConversionOptions.
	ConversionOptions
}

// ToSerializable converts a batch into a portable descriptor for distributed fetch.
// It captures the chunk address (inline bytes or URL + headers), the column metadata,
// timezone, and the conversion options that were captured when the batch was created.
//
// Call ToSerializable before Fetch: after a successful Fetch the in-memory records are
// released, though the descriptor itself remains valid for re-serialization.
func (rb *ArrowBatch) ToSerializable() (SerializableArrowBatch, error) {
	desc := rb.raw.Descriptor
	if desc.InlineDataBase64 == "" && desc.URL == "" {
		return SerializableArrowBatch{}, fmt.Errorf(
			"arrowbatches: batch has neither inline data nor a chunk URL and cannot be serialized")
	}

	name, offset := timezoneNameAndOffset(rb.raw.Location)

	return SerializableArrowBatch{
		Format:                arrowFormatName,
		InlineData:            desc.InlineDataBase64,
		URL:                   desc.URL,
		Headers:               desc.Headers,
		RowCount:              rb.raw.RowCount,
		UncompressedSize:      desc.UncompressedSize,
		RowTypes:              rb.rowTypes,
		TimezoneName:          name,
		TimezoneOffsetSeconds: offset,
		ConversionOptions:     rb.conversionOptions,
	}, nil
}

// arrowBatchOpts holds options for reconstructing an ArrowBatch on a worker node.
type arrowBatchOpts struct {
	client    *http.Client
	allocator memory.Allocator
}

// Option configures ToArrowBatch.
type Option func(*arrowBatchOpts)

// WithHTTPClient injects the HTTP client used to download remote chunks. It is optional:
// when not set, http.DefaultClient is used. Since the worker has no Snowflake connection to
// inherit transport settings from, this is the seam for configuring proxy/TLS/timeouts;
// note that http.DefaultClient has no timeout, so pass a client with one (or a Fetch context
// with a deadline) for remote batches.
func WithHTTPClient(client *http.Client) Option {
	return func(o *arrowBatchOpts) { o.client = client }
}

// WithAllocator sets the Arrow memory allocator used when decoding and transforming
// records. Defaults to memory.DefaultAllocator.
func WithAllocator(pool memory.Allocator) Option {
	return func(o *arrowBatchOpts) { o.allocator = pool }
}

// ToArrowBatch reconstructs an ArrowBatch from its serialized form on a worker node.
// The returned batch's Fetch downloads (if remote) and transforms the records exactly
// as a batch obtained directly from GetArrowBatches would.
//
// The HTTP client used for a remote batch defaults to http.DefaultClient; supply your own
// with WithHTTPClient to configure proxy/TLS/timeouts (recommended, since http.DefaultClient
// has no timeout). Inline batches perform no download. Call WithContext on the returned batch
// to bind a cancellation context to Fetch.
func (s SerializableArrowBatch) ToArrowBatch(opts ...Option) (*ArrowBatch, error) {
	if s.Format != "" && s.Format != arrowFormatName {
		return nil, fmt.Errorf("arrowbatches: unsupported serialized batch format %q", s.Format)
	}

	var o arrowBatchOpts
	for _, opt := range opts {
		opt(&o)
	}
	if o.allocator == nil {
		o.allocator = memory.DefaultAllocator
	}
	if o.client == nil {
		o.client = http.DefaultClient
	}

	client, alloc := o.client, o.allocator
	desc := ia.ChunkDescriptor{
		URL:              s.URL,
		Headers:          s.Headers,
		InlineDataBase64: s.InlineData,
		UncompressedSize: s.UncompressedSize,
	}

	return &ArrowBatch{
		rowTypes:          s.RowTypes,
		allocator:         alloc,
		ctx:               context.Background(),
		conversionOptions: s.ConversionOptions,
		raw: ia.BatchRaw{
			RowCount:   s.RowCount,
			Location:   loadLocation(s.TimezoneName, s.TimezoneOffsetSeconds),
			Descriptor: desc,
			Download: func(ctx context.Context) (*[]arrow.Record, int, error) {
				return ia.DownloadBatchRecords(ctx, client, desc, ipc.WithAllocator(alloc))
			},
		},
	}, nil
}

// timezoneNameAndOffset extracts a serializable timezone name and offset from a location.
func timezoneNameAndOffset(loc *time.Location) (string, int) {
	if loc == nil {
		return "", 0
	}
	_, offset := time.Now().In(loc).Zone()
	return loc.String(), offset
}

// loadLocation reconstructs a *time.Location from a serialized name and offset. Named
// IANA zones are resolved via the tz database when available; fixed-offset zones (e.g.
// "+0300"), the process-local "Local", and any name the worker cannot resolve fall back
// to a fixed zone built from the stored offset. Named zones with DST transitions require
// tzdata on the worker for full fidelity.
func loadLocation(name string, offsetSeconds int) *time.Location {
	if name == "" || name == "Local" || strings.HasPrefix(name, "+") || strings.HasPrefix(name, "-") {
		return time.FixedZone(name, offsetSeconds)
	}
	if loc, err := time.LoadLocation(name); err == nil {
		return loc
	}
	return time.FixedZone(name, offsetSeconds)
}
